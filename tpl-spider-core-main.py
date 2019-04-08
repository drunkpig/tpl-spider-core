import asyncio
import sys
from psycopg2.extensions import TransactionRollbackError
from psycopg2 import DatabaseError,ProgrammingError,OperationalError
from config import logger
import threading
import config
import config as dbconfig
import json
from schedule_task import clean_timeout_temp_dir_and_archive
from template_crawl import TemplateCrawler
import psycopg2
import random
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone
from utils import send_template_mail


class SpiderTask(object):
    def __get_task_by_sql(self, sql):
        db_trans = psycopg2.connect(database=dbconfig.db_name, user=dbconfig.db_user, password=dbconfig.db_psw,
                                    host=dbconfig.db_url, port=dbconfig.db_port)
        db_trans.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ)
        cursor = db_trans.cursor()

        try:
            cursor.execute(sql)
            if cursor.rowcount>0:
                row = cursor.fetchone()
                db_trans.commit()
            else:
                return None
        except TransactionRollbackError as multip_update_exp:
            logger.info(multip_update_exp)
            db_trans.rollback()
            return None
        except (DatabaseError, ProgrammingError, OperationalError) as dbe:
            logger.info(dbe)
            db_trans.rollback()
            return None
        finally:
            db_trans.close()

        if row is None:
            return None

        r = row
        cursor.close()
        task = {
            'id': r[0],
            'seeds': json.loads(r[1]),
            'ip': r[2],
            'email': r[3],
            'user_agent': r[4],
            'status': r[5],
            'is_grab_out_link': r[6],
            'is_to_single_page': r[7],
            'is_full_site':r[8],
            'is_ref_model':r[9],
            'gmt_modified': r[10],
            'gmt_created': r[11],
            'file_id': r[12],
            'encoding':r[13],
            'to_framework':r[14],
        }

        return task

    def __get_timeout_task(self):
        sql = f"""
            -- start transaction isolation level repeatable read;
            update spider_task set gmt_modified = NOW() where id in (
                select id
                from spider_task
                where status ='P' AND gmt_modified + '{config.max_task_run_tm_seconds} seconds'::INTERVAL < NOW()
                order by gmt_created DESC 
                limit 1
            )
            returning id, seeds, ip, email, user_agent, status, is_grab_out_link, is_to_single_page, is_full_site, is_ref_model, gmt_modified, gmt_created,file_id, encoding,to_framework;
            -- commit;
        """
        return self.__get_task_by_sql(sql)

    def __get_a_task(self):
        sql = """
            -- start transaction isolation level repeatable read;
            update spider_task set status = 'P', gmt_modified=NOW() where id in (
                select id
                from spider_task
                where status ='I'
                order by gmt_created DESC 
                limit 1
            )
            returning id, seeds, ip, email, user_agent, status, is_grab_out_link,is_to_single_page, is_full_site, is_ref_model, gmt_modified, gmt_created, file_id, encoding, to_framework;
            -- commit;
        """
        return self.__get_task_by_sql(sql)

    def __update_task_status(self, task_id, zip_path, status='C'):
        db = psycopg2.connect(database=dbconfig.db_name, user=dbconfig.db_user, password=dbconfig.db_psw,
                              host=dbconfig.db_url, port=dbconfig.db_port)
        try:
            sql = f"""
                update spider_task set status = '{status}', result='{zip_path}' where id = '{task_id}';
            """
            cursor = db.cursor()
            logger.info("begin execute sql %s", sql)
            cursor.execute(sql)
            cursor.close()
            db.commit()
        except Exception as e:
            logger.exception(e)
        finally:
            if db:
                db.close()

    def __get_user_agent(self, key):
        ua_list = config.ua_list.get(key)
        if ua_list is None:
            ua = config.default_ua
        else:
            return random.choice(ua_list)

        return ua

    async def loop(self, base_craw_file_dir):
        while True:
            task = self.__get_timeout_task()  # 优先处理超时的任务

            if task is not None:
                logger.info("获得一个超时任务 %s", task['id'])
            else:
                task = self.__get_a_task()
                if not task:
                    logger.info("no task, wait")
                    await asyncio.sleep(config.wait_db_task_interval_s)
                    continue
                else:
                    logger.info("获得一个正常任务 %s", task['id'])

            seeds = task['seeds']
            task_id = task['id']
            is_grab_out_site_link = task['is_grab_out_link'] #是否抓取外部站点资源
            is_to_single_page = task['is_to_single_page']
            is_full_site = task['is_full_site']
            is_ref_model = task['is_ref_model']
            encoding = task['encoding']
            to_framework = task['to_framework']
            user_agent = self.__get_user_agent(task['user_agent'])
            spider = TemplateCrawler(seeds, save_base_dir=f"{base_craw_file_dir}/",
                                     header={'User-Agent': user_agent},
                                     encoding=encoding,
                                     grab_out_site_link=is_grab_out_site_link,
                                     to_single_page=is_to_single_page,
                                     full_site=is_full_site,
                                     ref_model=is_ref_model,
                                     framework=to_framework
                                     )
            template_zip_file = await spider.template_crawl()
            try:
                await asyncio.wait_for(spider.template_crawl(), timeout=config.max_task_run_tm_seconds)
            except asyncio.TimeoutError:
                #TODO #发给用户提示超时
                self.__update_task_status(task_id, "", "E")

            logger.info("begin update task finished")
            self.__update_task_status(task_id, template_zip_file)
            send_template_mail("web template download link", "email-download.html", {"{{template_id}}":task['file_id']}, task['email'])
            logger.info("send email to %s, link: %s", task['email'], task['file_id'])


def setup_schedule_task(n_days_age, search_parent_dir_list):
    time_zone = timezone("Asia/Shanghai")
    scheduler = BackgroundScheduler(timezone=time_zone)
    trigger = CronTrigger.from_crontab(config.delete_file_cron, timezone=time_zone)
    scheduler.add_job(clean_timeout_temp_dir_and_archive, trigger, kwargs={"n_day": n_days_age, "parent_dir_list":search_parent_dir_list})
    # 启动时清理一下
    clean_timeout_temp_dir_and_archive(n_days_age, search_parent_dir_list)

async def main(base_craw_file_dir):
    await asyncio.gather(SpiderTask().loop(base_craw_file_dir),
                         SpiderTask().loop(base_craw_file_dir),
                         SpiderTask().loop(base_craw_file_dir),
                         SpiderTask().loop(base_craw_file_dir))


if __name__ == "__main__":
    logger.info("tpl-spider-web start, thread[%s]"% threading.current_thread().getName())
    base_craw_file_dir = sys.argv[1]
    logger.info("基本目录是%s", base_craw_file_dir)
    if not base_craw_file_dir:
        logger.error("没有指明模版压缩文件的目录")
        exit(-1)

    setup_schedule_task(config.delete_file_n_days_age, [f'{base_craw_file_dir}/{config.template_temp_dir}', f'{base_craw_file_dir}/{config.template_archive_dir}'])
    asyncio.run(main(base_craw_file_dir))
