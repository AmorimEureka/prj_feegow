import dlt
from prj_dlt_feegow.source_feegow import feegow_source
from datetime import datetime, timedelta
from typing import Any


def pipeline_run():

    from pathlib import Path
    import json
    from dateutil.relativedelta import relativedelta

    pipeline = dlt.pipeline(
        pipeline_name="feegow_agenda",
        destination="postgres",
        dataset_name="raw_feegow"
    )

    ARQUIVO_ESTADO = Path(".dlt/pipeline_state/feegow_state.json")

    if not ARQUIVO_ESTADO.exists():

        # VARIAVEL PARA RECEBER ULTIMA DATA ATE ONDE OS DADOS FORAM CARREGADOS
        estado: Any = {}

        # DATA ATUAL MAIS 90 DIAS SE ESSA DATA FOR DENTRO DO ANO ATUAL
        # CASO CONTRARIO RETORNA ULTIMA DATA DO ANO
        # PARA EVITAR RETORNAR PRIMEIRO DIA DO ANO SEGUINTE
        dt_agenda = min(datetime.now() + timedelta(days=90), datetime.now() + relativedelta(month=12, day=31))
        primeira_data_ano_atual = dt_agenda + relativedelta(month=1, day=1)
        dt_inicial = primeira_data_ano_atual

        contagem_periodo = 1
        modo_escrita = "append"

        # BATCH DE EXTRACAO COM DADOS DE 30 EM 30
        while dt_inicial < dt_agenda:
            dt_seguinte = min(dt_inicial + timedelta(days=30), dt_agenda)
            p_dt_inicial = dt_inicial.strftime("%Y-%m-%d")

            pipeline.run(
                feegow_source(primeira_data=p_dt_inicial, p_dias_futuro=30, write_disposition=modo_escrita)
                )

            dt_inicial = dt_seguinte

            contagem_periodo += 1

        # SALVA ULTIMA DATA ATE ONDE OS DADOS FORAM EXTRAIDOS NO JSON
        ARQUIVO_ESTADO.parent.mkdir(parents=True, exist_ok=True)
        with open(ARQUIVO_ESTADO, "w") as w:
            estado["ultima_data_carregada"] = dt_seguinte.strftime("%Y-%m-%d")
            json.dump(estado, w)
    else:

        with open(ARQUIVO_ESTADO, "r") as r:
            estado = json.load(r)
            dt_ultimo_carregamento = estado.get("ultima_data_carregada")
            dt_final = datetime.strptime(dt_ultimo_carregamento, "%Y-%m-%d")

            dt_atual = datetime.now()
            dias_diff = dt_atual - dt_final

            dias = dias_diff.days
            p_dt_inicial = dt_atual.strftime("%Y-%m-%d")
            modo_escrita = "merge"

            if dt_atual < dt_final:

                print(f"PIPELINE SERÁ EXECUTADO NO MODO {modo_escrita} DE {p_dt_inicial} ATÉ {dt_final}")

                pipeline.run(
                    feegow_source(primeira_data=p_dt_inicial, p_dias_futuro=dias, write_disposition=modo_escrita)
                    )


if __name__ == "__main__":
    pipeline_run()
