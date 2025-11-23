import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Any, Dict

import dlt
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from dlt.common import pendulum


default_task_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": "test@test.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": timedelta(hours=20),
}



# Configurar credenciais do PostgreSQL
pg_hook = PostgresHook(postgres_conn_id="conn_dw_postgres_local")
conn = pg_hook.get_connection(pg_hook.postgres_conn_id)
os.environ["DESTINATION__CREDENTIALS"] = (
    f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
)
# Configurar token Feegow
os.environ["FEEGOW_TOKEN"] = Variable.get("feegow_token")




@dag(
    dag_id="feegow_pipeline_dag",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_task_args,
)
def load_feegow_data():
    """
    DAG para carregar dados do Feegow com controle de estado via XCom.
    - Primeira execução: carrega dados em batches de 30 dias do início do ano até hoje + 90 dias
    - Execuções subsequentes: faz merge/upsert dos próximos 90 dias a partir da última data carregada
    """

    @task
    def verifica_estado_calcula_periodo_modo_escrita(**context) -> Dict[str, Any]:
        """
        Verifica se existe estado anterior (via XCom) e calcula os períodos de extração.
        Retorna configuração para execução em batch ou merge.
        """

        ultima_data_carregada = None
        try:
            ti = context['ti']
            ultima_data_carregada = ti.xcom_pull(
                task_ids='salvar_estado_final',
                key='return_value',
                include_prior_dates=True
            )
            print(f"Última data recuperada - XCom: {repr(ultima_data_carregada)}")

        except Exception as e:
            print(f"Não consegui recuperar XCom anterior: {e}")
            ultima_data_carregada = None

        dt_atual = datetime.now()

        if not ultima_data_carregada or ultima_data_carregada is None:

            print("Primeira execução - carregando do histórico em batches de 30 dias")

            dt_agenda = min(
                            dt_atual + timedelta(days=90),
                            dt_atual + relativedelta(month=12, day=31)
                        )

            dt_inicial = dt_agenda + relativedelta(month=1, day=1)

            batches = []
            while dt_inicial < dt_agenda:
                dt_seguinte = min(dt_inicial + timedelta(days=30), dt_agenda)

                batches.append({
                    'dt_inicial': dt_inicial.strftime("%Y-%m-%d"),
                    'dt_final': dt_seguinte.strftime("%Y-%m-%d"),
                    'dias': (dt_seguinte - dt_inicial).days
                })

                dt_inicial = dt_seguinte

            print(f"Total de batches calculados: {len(batches)}")

            for i, batch in enumerate(batches, 1):
                print(f"  Batch {i}: {batch['dt_inicial']} até {batch['dt_final']} ({batch['dias']} dias)")

            return {
                'modo': 'primeira_execucao',
                'write_disposition': 'merge',
                'batches': batches,
                'ultima_data_periodo': dt_agenda.strftime("%Y-%m-%d")
            }

        else:
            # Merger Upsert - Data atual até ultima data de carga
            dt_final = datetime.strptime(ultima_data_carregada['ultima_data_carregada'], "%Y-%m-%d")

            p_dt_inicial = dt_atual.strftime("%Y-%m-%d")
            dias_diff = (dt_final - dt_atual).days

            # Se a última data carregada for no futuro, começar de hoje
            if dt_atual < dt_final:

                print(f"Modo de carregamento MERGE: {p_dt_inicial} até {dt_final.strftime('%Y-%m-%d')} - ({dias_diff} dias)")

                return {
                    'modo': 'merge',
                    'write_disposition': 'merge',
                    'batches': [{
                        'dt_inicial': p_dt_inicial,
                        'dt_final': dt_final.strftime("%Y-%m-%d"),
                        'dias': dias_diff
                    }],
                    'ultima_data_periodo': dt_final.strftime("%Y-%m-%d")
                }

    # Verificar estado e calcular os períodos
    config = verifica_estado_calcula_periodo_modo_escrita()

    @task
    def executar_pipeline_em_batches(config: Dict[str, Any], **context):

        from prj_dlt_feegow.source_feegow import feegow_source

        batches = config['batches']
        write_disposition = config['write_disposition']
        modo = config['modo']

        print(f"Iniciando pipeline no modo: {modo}")
        print(f"Write disposition: {write_disposition}")
        print(f"Total de batches: {len(batches)}")

        pipeline = dlt.pipeline(
            pipeline_name="feegow_agenda",
            dataset_name="raw_feegow",
            destination="postgres",
            full_refresh=False,
        )

        for i, batch in enumerate(batches, 1):
            print(f"\n{'='*60}")
            print(f"Batch {i}/{len(batches)}")
            print(f"Período: {batch['dt_inicial']} até {batch['dt_final']}")
            print(f"Dias: {batch['dias']}")
            print(f"{'='*60}")

            source = feegow_source(
                primeira_data=batch['dt_inicial'],
                p_dias_futuro=batch['dias'],
                write_disposition=write_disposition
            )

            pipeline.run(source)

            ultima_data_batch = batch['dt_final']
            context['ti'].xcom_push(
                key='ultima_data_carregada',
                value=ultima_data_batch
            )

        return {
            'batches_processados': len(batches),
            'modo': modo,
            'write_disposition': write_disposition,
            'ultima_data_periodo': batches[-1]['dt_final']
        }

    @task
    def salvar_estado_final(resultado_pipeline: Dict[str, Any], **context):
        """
        Tarefa responsavel por salvar o estado após o último batch;
        que será recuperado na próxima execução da DAG.
        """
        ultima_data = resultado_pipeline.get('ultima_data_periodo')
        if ultima_data:
            estado = {
                'ultima_data_carregada': ultima_data,
                'batches_processados': resultado_pipeline['batches_processados'],
                'modo': resultado_pipeline['modo']
            }
            return estado

        return None

    resultado_pipeline = executar_pipeline_em_batches(config)
    estado_final = salvar_estado_final(resultado_pipeline)

    config >> resultado_pipeline >> estado_final


load_feegow_data()


# import os
# from datetime import timedelta

# import dlt
# from airflow.decorators import dag
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.models import Variable
# from dlt.common import pendulum
# from dlt.helpers.airflow_helper import PipelineTasksGroup


# default_task_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "email": "test@test.com",
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "retries": 0,
#     "execution_timeout": timedelta(hours=20),
# }


# @dag(
#     dag_id="feegow_pipeline_dag",
#     schedule="@daily",
#     start_date=pendulum.datetime(2025, 1, 1),
#     catchup=False,
#     max_active_runs=1,
#     default_args=default_task_args,
# )
# def load_feegow_data():
#     # store postgres credentials in env variable from Airflow Connection
#     pg_hook = PostgresHook(postgres_conn_id="conn_dw_postgres_local")
#     conn = pg_hook.get_connection(pg_hook.postgres_conn_id)
#     os.environ["DESTINATION__CREDENTIALS"] = (
#         f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
#     )

#     # store feegow token in env variable from Airflow Variable
#     os.environ["FEEGOW_TOKEN"] = Variable.get("feegow_token")

#     # set `use_data_folder` to True to store temporary data on the `data` bucket.
#     # Use only when it does not fit on the local storage
#     tasks = PipelineTasksGroup(
#         "feegow_pipeline_decomposed", use_data_folder=False, wipe_local_data=True
#     )

#     # import your source from pipeline script
#     from prj_dlt_feegow.pipeline_feegow import pipeline_run

#     # modify the pipeline parameters
#     pipeline = dlt.pipeline(
#         pipeline_name="feegow_agenda",
#         dataset_name="raw_feegow",
#         destination="postgres",
#         full_refresh=False,  # must be false if we decompose
#         refresh="drop_sources",
#     )

#     # get the source with date logic from pipeline_feegow
#     source = pipeline_run()

#     # the "serialize" decompose option will converts
#     # dlt resources into Airflow tasks. use "none" to disable it
#     tasks.add_run(
#         pipeline,
#         source,
#         decompose="serialize",
#         trigger_rule="all_done",
#         retries=0,
#     )


# load_feegow_data()
