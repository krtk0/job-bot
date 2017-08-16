import requests
from lxml import html
import re
import sqlite3
from telegram.ext import Updater, CommandHandler, Job
import logging
from datetime import datetime

"Enable logging"
logger = logging.getLogger()
logger.setLevel(logging.INFO)

"Get a telegram bot token"
TOKEN = ''
with open('PATH TO TOKEN', 'r') as token:
    for line in token:
        TOKEN += line


class Connection:
    """
    Connection with the database
    """
    def __init__(self):
        """
        Open database connection
        """
        self.dbc = sqlite3.connect('PATH TO SQLite DATABASE')
        self.dbc.row_factory = sqlite3.Row
        self.c = self.dbc.cursor()

    def do(self, sql, *args):
        """
        Execute SQL query
        """
        result = self.c.execute(sql, tuple(args)).fetchall()
        self.dbc.commit()
        return [dict(row) for row in result]

    def close(self):
        """
        Close database connection
        """
        self.dbc.close()


class Query:
    """
    SQL query methods
    """
    @classmethod
    def is_job_new(cls, dbc, job_id):
        """
        Check whether job is newposted
        """
        sql = """
            SELECT job_id
            FROM jobs
            WHERE job_id=?
        """
        result = dbc.do(sql, job_id)
        return False if len(result) > 0 else True

    @classmethod
    def add_job(cls, dbc, job_id, job_title):
        """
        Add a new job to the database
        """
        sql = """
            INSERT INTO jobs (job_id, job_title) VALUES (?, ?)
        """
        dbc.do(sql, job_id, job_title)
        logging.info('{0:%Y-%b-%d %H:%M:%S} Job {1} added'.format(datetime.now(), job_id))

    @classmethod
    def add_to_sublist(cls, dbc, chat_id):
        """
        Add user to the sublist
        """
        sql = """
            INSERT INTO subscribers (chat_id) VALUES (?)
        """
        return Connection.do(dbc, sql, chat_id)

    @classmethod
    def remove_from_sublist(cls, dbc, chat_id):
        """
        Remove user from the sublist
        """
        sql = """
                DELETE
                FROM subscribers
                WHERE chat_id = ?
            """
        return Connection.do(dbc, sql, chat_id)

    @classmethod
    def get_subs(cls, dbc):
        """
        Get all subscribers
        """
        sql = """
            SELECT *
            FROM subscribers
        """
        return Connection.do(dbc, sql)

    @classmethod
    def get_sub_one(cls, dbc, chat_id):
        """
        Get subscriber by his chat_id
        """
        sql = """
            SELECT *
            FROM subscribers
            WHERE chat_id = ?
        """
        return Connection.do(dbc, sql, chat_id)


def parse_jobs(bot, job):
    """
    Parse the VUB student jobs website to find new posted jobs
    and push them to Telegram
    """
    # bot.send_message(chat_id=update.message.chat_id, text='You will be notified about newposted student jobs at VUB.')
    dbc = Connection()
    # logging.info('Connection with DB established')
    url = 'http://jobs.vub.ac.be/jobs'
    page = requests.get(url).text
    doc = html.document_fromstring(page)

    "Get jobs' ids from the first page"
    ids = []
    for spam in doc.xpath("//*[contains(@class, 'views-field views-field-nid')]"):
        job_id = re.search(re.compile(r'\d\d\d\d'), spam.text)
        if job_id:
            ids.append(job_id.group(0))

    "Get jobs' titles from the first page"
    titles = []
    for spam in doc.xpath("//*[contains(@class, 'views-field views-field-title')]/a"):
        titles.append(spam.text)
    titles = titles[1:]

    "Create jobs dictionary: {job_id: job:title}"
    # jobs_dict = dict(zip(ids, titles))

    _job_new = True
    i = 0
    while i < len(ids) and _job_new:
        url_job = 'http://jobs.vub.ac.be/node/'
        _job_new = Query.is_job_new(dbc, ids[i])
        if _job_new:
            Query.add_job(dbc, ids[i], titles[i])
            url_job += ids[i]
            logging.info('{0:%Y-%b-%d %H:%M:%S} New job is posted: {1}'.format(datetime.now(), url_job))
            subs = Query.get_subs(dbc)
            if subs:
                _count = 0
                for sub in subs:
                    "Push to Telegram"
                    bot.send_message(chat_id=sub['chat_id'], text='New job "{0}" is posted at {1}'.format(titles[i], url_job))
                    _count += 1
                logging.info('{2:%Y-%b-%d %H:%M:%S} Notification about the job {0} is sent to {1} user(s)'.format(ids[i],
                                                                                                                  _count,
                                                                                                                  datetime.now()))
        else:
            logging.info('{0:%Y-%b-%d %H:%M:%S} Nothing new found'.format(datetime.now()))
        i += 1

    i = None
    _job_new = None
    dbc.close()
    # logging.info('Connection with DB closed')


def start_com(bot, update):
    """
    COMMAND /start
    Subscribe user for job updates
    """
    dbc = Connection()
    on_sublist = Query.get_sub_one(dbc, str(update.message.chat_id))
    if not on_sublist:
        Query.add_to_sublist(dbc, str(update.message.chat_id))
        logging.info('{1:%Y-%b-%d %H:%M:%S} User {0} is added on the sublist.'.format(update.message.chat_id,
                                                                                      datetime.now()))
        bot.send_message(chat_id=update.message.chat_id, text='You will be notified about newposted student jobs at VUB.')
    else:
        bot.send_message(chat_id=update.message.chat_id, text='You are already subscribed.')
    dbc.close()


def stop_com(bot, update):
    """
    Unsubscribe user from job updates
    """
    dbc = Connection()
    Query.remove_from_sublist(dbc, str(update.message.chat_id))
    logging.info('{1:%Y-%b-%d %H:%M:%S} User {0} is removed from the sublist.'.format(update.message.chat_id,
                                                                                      datetime.now()))
    bot.send_message(chat_id=update.message.chat_id, text='You canceled your subscription successfully.'
                                                              '\nSend me /start to subscribe again.')
    dbc.close()


def help_com(bot, update):
    """
    COMMAND /help
    """
    bot.send_message(chat_id=update.message.chat_id, text='Something helpful should be here.'
                                                          '\n/start — subscribe for updates'
                                                          '\n/stop – unsubscribe from updates'
                                                          '\n/help – obviously')


def main():
    updater = Updater(TOKEN)
    dp = updater.dispatcher
    j = updater.job_queue

    dp.add_handler(CommandHandler('start', start_com))
    dp.add_handler(CommandHandler('help', help_com))
    dp.add_handler(CommandHandler('stop', stop_com))
    j.put(Job(parse_jobs, 60.0), next_t=0.00)

    updater.start_polling()
    updater.idle()


if __name__ == '__main__':
    main()
