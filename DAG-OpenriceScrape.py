from datetime import timedelta
from airflow.operators.python_operator import PythonVirtualenvOperator

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.utils.dates import days_ago
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization


def callable_virtualenv(ts):
    import requests as rq
    from bs4 import BeautifulSoup as BS4
    import pandas as pd
    from google.cloud import storage

    hd = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    url = "https://www.openrice.com/zh/hongkong/restaurants?landmarkId=9009"
    rp = rq.get(url, headers=hd)
    # print(rp.text)

    soup = BS4(rp.text, 'html.parser')
    # print(soup.prettify())
    ul = soup.find_all("ul", class_="sr1-listing-content-cells pois-restaurant-list js-poi-list-content-cell-container")[0]
    lis = ul.select("li.sr1-listing-content-cell")
    temp = []
    for li in lis:
        shop_id = li.select("[data-poi-id]")[0]
        shop_name = li.select(".title-name")[0]
        address = li.select(".address span")[0]
        price = li.select(".icon-info-food-price")[0]
        like = li.select(".smile-face .score")[0]
        if len(li.select("div.promotion")) == 1:
            is_health = True
        else:
            is_health = False

        temp.append([shop_id['data-poi-id'].strip(), shop_name.text.strip(), address.text.strip(), price.text.strip(),
                     like.text])

    pd.DataFrame(temp).to_csv(f'scraped-{ts}.csv')

    storage_client = storage.Client()
    bucket = storage_client.bucket("storage_mika_csv")
    blob = bucket.blob(f'scraped-{ts}.csv')

    blob.upload_from_filename(f'scraped-{ts}.csv')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
with DAG(
    'Scraping',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['scraped-example'],

) as dag:
    virtualenv_task = PythonVirtualenvOperator(
        task_id="virtualenv_python",
        python_callable=callable_virtualenv,
        requirements=["requests", "BeautifulSoup4", "pandas", "google-cloud-storage"],
        system_site_packages=False,
        op_kwargs={'ts':'{{ ts }}'}
    )

# use composer bucket to schedule DAG ( scraping data as csv and push it to storage bucket)