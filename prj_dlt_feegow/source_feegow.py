import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()


@dlt.source(name="feegow_agenda")
def feegow_source(
    feegow_token: str = None,
    primeira_data: str = None,
    p_dias_futuro: int = 1,
    write_disposition: str = "append"
):

    data_comeco = datetime.strptime(primeira_data, "%Y-%m-%d")
    data_end = data_comeco + timedelta(days=p_dias_futuro)

    dt_inicial = primeira_data
    dt_final = data_end.strftime("%Y-%m-%d")

    # OBTER TOKEN DA API FEEGOW
    if feegow_token is None:
        feegow_token = os.getenv("feegow_token")
        if not feegow_token:
            raise ValueError("Não encontrei o token:'feegow_token' no arquivo .env")

    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.feegow.com/v1/api/",
            "headers": {
                "X-Access-Token": feegow_token,
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
        },
        "resource_defaults": {
            "endpoint": {
                "method": "GET",
                "response_actions": [
                    {"status_code": 404, "action": "ignore"},
                    {"status_code": 409, "action": "ignore"}
                ]
            }
        },
        "resources": [
            {
                "name": "agendamentos",
                "write_disposition": write_disposition,
                "primary_key": "agendamento_id" if write_disposition == "merge" else None,
                "endpoint": {
                    "path": "appoints/search",
                    "params": {
                        "data_start": dt_inicial,
                        "data_end": dt_final,
                        "list_procedures": 1,
                        "start": 0,
                        "offset": 50
                    },
                    "data_selector": "content",
                    "paginator": {
                        "type": "offset",
                        "limit": 50,
                        "offset_param": "start",
                        "limit_param": "offset"
                    }
                },
            },
            {
                "name": "status",
                "write_disposition": {
                    "disposition": "replace"
                },
                "primary_key": ["id"],
                "endpoint": {
                    "path": "appoints/status",
                    "data_selector": "content"
                }
            },
            {
                "name": "motivos",
                "write_disposition": {
                    "disposition": "replace"
                },
                "primary_key": ["id"],
                "endpoint": {
                    "path": "appoints/motives",
                    "data_selector": "content"
                }
            },
            {
                "name": "canais",
                "write_disposition": {
                    "disposition": "replace"
                },
                "primary_key": ["id"],
                "endpoint": {
                    "path": "appoints/list-channel",
                    "data_selector": "content"
                }
            },
            {
                "name": "pacientes",
                "write_disposition": {
                    "disposition": "replace"
                },
                "primary_key": ["patient_id"],
                "endpoint": {
                    "path": "patient/list",
                    "data_selector": "content"
                }
            },
            {
                "name": "procedimentos",
                "write_disposition": {
                    "disposition": "replace"
                },
                "primary_key": ["procedimento_id"],
                "endpoint": {
                    "path": "procedures/list",
                    "data_selector": "content"
                }
            },
            {
                "name": "profissionais",
                "write_disposition": {
                    "disposition": "replace"
                },
                "primary_key": ["profissional_id"],
                "endpoint": {
                    "path": "professional/list",
                    "data_selector": "content"
                }
            },
            {
                "name": "locais",
                "write_disposition": {
                    "disposition": "replace"
                },
                "primary_key": ["id"],
                "endpoint": {
                    "path": "company/list-local",
                    "data_selector": "content"
                }
            },
            {
                "name": "convenios",
                "write_disposition": {
                    "disposition": "replace"
                },
                "primary_key": ["convenio_id"],
                "endpoint": {
                    "path": "insurance/list",
                    "data_selector": "content"
                }
            },
            {
                "name": "unidades",
                "write_disposition": {
                    "disposition": "replace"
                },
                "primary_key": ["unidade_id"],
                "endpoint": {
                    "path": "company/list-unity",
                    "data_selector": "content.unidades"
                }
            },
            {
                "name": "matriz",
                "write_disposition": {
                    "disposition": "replace"
                },
                "primary_key": ["unidade_id"],
                "endpoint": {
                    "path": "company/list-unity",
                    "data_selector": "content.matriz"
                }
            },
            {
                "name": "especialidades",
                "write_disposition": {
                    "disposition": "replace"
                },
                "primary_key": ["especialidade_id"],
                "endpoint": {
                    "path": "specialties/list",
                    "data_selector": "content"
                }
            }


        ]
    }

    yield from rest_api_resources(config)
