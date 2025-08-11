# Airflow
## Env
* python version: 3.10.14

### Install python 3.10.4 
```shell
pyenv install 3.10.14
pyenv local 3.10.14
```

### Create python venv
```shell
python3.10 -m venv .venv
source .venv/bin/activate
```

> Note
> Check python path and version
> ```shell
> which python
> python --version
> ```

## Installation
### Set AIRFLOW_HOME
Create a directory for airflow home directory whose config file(e.g. airflow.cfg) and logs. After that, set the directory as `AIRFLOW_HOME`.
```shell
export AIRFLOW_HOME=[path of the directory]
```

> Example
> 
> ```shell
> export AIRFLOW_HOME=$(pwd)/airflow-home
> ```

### Setup constraint url
```shell
AIRFLOW_VERSION=2.9.0
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
```

### Install airflow
```shell
pip install "apache-airflow[postgres,celery]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

> Note
> If there is any problem then install airflow without any plugin first, then install plugins.
> ```shell
> pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
> pip install "apache-airflow[postgres,celery]" --constraint "${CONSTRAINT_URL}"
> ```
> Please remember that `--constraint` has to be in second installation

### Setup airflow database (PostgreSQL)
```sql
CREATE USER airflow WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

ALTER DATABASE airflow OWNER TO airflow;
ALTER ROLE airflow SET client_encoding TO 'utf8';
ALTER ROLE airflow SET default_transaction_isolation TO 'read committed';
ALTER ROLE airflow SET timezone TO 'Asia/Seoul';
```

### Init airflow
First check version of the installed airflow, and it will be `2.9.0`
```shell
airflow version
```
#### Init airflow DB
```shell
airflow db init
```
After run `db init`, should check there are files and directories in $AIRFLOW_HOME.
* airflow.cfg
* webserver_config.py (optional, if it is not, it's fine)
* logs

#### Create admin user
```shell
airflow users create --username admin --role Admin --firstname admin --lastname user --email admin@example.com
```

### Update airflow config
#### Modify airflow.cfg
```text
[core]
executor = LocalExecutor

[database]
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@localhost:25432/airflow
```

> Note
> After updating `airflow.cfg`, should run `airflow db init` again.

## Run
### Config env variables
#### AIRFLOW_HOME
```shell
export AIRFLOW_HOME=[path of directory where airflow.cfg file]
```
#### PYTHONPATH
```shell
export PYTHONPATH=[directory of project root directory whose .venv(or venv) directory]
```
  
### run scheduler
```shell
airflow scheduler
```
### run web-server
```shell
airflow webserver --port [port number]
```

## ETC
### NO sample dags
If you don't need sample dags, then update airflow.cfg like below.
```text
[core]
load_samples = False
```

### Uninstall
#### Uninstall airflow
```shell
pip uninstall apache-airflow
```

### Reset airflow DB
```shell
airflow db reset
```

#### Uninstall packages
```shell
pip freeze | grep apache-airflow | xargs pip uninstall -y
```

#### Remove airflow home directory
```shell
rm -rf ~/airflow
```

## Refs
### Installation
* [How to install Airflow: For Apple M2](https://swift-tree.dev/how-to-install-airflow-for-apple-m2/)

## Trouble shoot
### WARNING: There was an error checking the latest version of pip.
#### certifi issue
```shell
pip install --upgrade certifi
```

#### Proxy issue
```shell
pip install --proxy=http://your.proxy:port --upgrade pip
```
