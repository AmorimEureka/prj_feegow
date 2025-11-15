Projeto dlthub - Manipulação API Feegow
===

<br>
<br>

<details open>
    <summary><strong>PROJETO</strong></summary>
    <p></p>

<ul style="list-style-type: none;">

### **Extrair dados de endpoints da [API feegow](https://staging-docs.feegow.com/) com contexto de agendamentos e carregá-los no BigQuery para posterior tratamento e modelagem de dados para criação de Dashboards em PowerBI**



<br>


<details open>

<summary><strong>STACKS UTILIZADAS</strong></summary>

<br>

![Python](https://img.shields.io/badge/Python-FFD43B?&logo=python&logoColor=blue)
![Airflow 2.8.0](https://img.shields.io/badge/Airflow-2.8.0-EA1D2C?logo=apache-airflow&logoColor=white)
[![Astro](https://img.shields.io/badge/Astro-Astronomer.io-5A4FCF?logo=Astronomer&logoColor=white)](https://www.astronomer.io/)
[![cosmos version](https://img.shields.io/pypi/v/astronomer-cosmos?label=cosmos&color=purple&logo=apache-airflow)](https://pypi.org/project/astronomer-cosmos/) <br>
![dlt version](https://img.shields.io/pypi/v/dlt?label=dlt&color=blue&logo=python&logoColor=white)
![dbt version](https://img.shields.io/pypi/v/dbt-core?label=dbt-core&color=orange&logo=databricks&logoColor=white)
![Docker Engine](https://img.shields.io/badge/Docker-Engine-2496ED?logo=docker&logoColor=white)
![Ubuntu](https://img.shields.io/badge/OS-Ubuntu-E95420?logo=ubuntu&logoColor=white) <br>
![WSL](https://img.shields.io/badge/WSL-2.0+-brightgreen?logo=windows&logoColor=white)
![Postgres](https://img.shields.io/badge/postgres-%23316192.svg?&logo=postgresql&logoColor=white)
[![DBeaver](https://img.shields.io/badge/DBeaver-Tool-372923?logo=dbeaver&logoColor=white)](https://dbeaver.io/)
[![API Feegow](https://img.shields.io/badge/API-Feegow-blue?logo=fastapi&logoColor=white)](https://documenter.getpostman.com/view/3897235/S1ENwy59)
![Power Bi](https://img.shields.io/badge/power_bi-F2C811?&logo=powerbi&logoColor=black)


</details>

<br>

</ul>

</details>



<br>

<details>
<summary><strong>CONFIGURAÇÃO DO AMBIENTE</strong></summary>

<br>

<ul style="list-style-type: none;">

<li>

Configurar Github

```bash
git init

git branch -m main

```

</li>


<li>

Definir versão Python do projeto:

```bash
pyenv version

pyenv local 3.12.1

pyenv versions
```

</li>


<li>

Criar Ambiente Virual - Poetry:

```bash
# -- Se criou projeto com mkdir
    poetry init

# ou

# -- repo do zero
    poetry new <Nome_Novo_Porjeto>

# -- habilita gerenciamento do projeto p/ poetry
    poetry config virtualenvs.in-project true


# -- Informa versão python definida pelo Pyenv
    poetry env use 3.12.1

# -- Ativa o ambiente virtual
    source .venv/bin/activate
```

</li>


<li>

Instalar Library dlt

```bash
poetry add dlt --python "^3.12"
```

</li>


</ul>

<br>

___

</details>




