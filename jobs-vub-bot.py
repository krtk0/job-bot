import requests
from lxml import html
import re
import psycopg2
import psycopg2.extras
from telegram.ext import Updater, CommandHandler, MessageHandler, Job, Filters
import logging
from datetime import datetime

"Enable logging"
logger = logging.getLogger()
logger.setLevel(logging.INFO)

"Get a telegram bot token"
TOKEN = ''
with open(TOKENPATH, 'r') as token:
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
        self.dbc = psycopg2.connect(CONNECTION_PARAMS)
        self.c = self.dbc.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def do(self, sql, *args):
        """
        Execute SQL query
        """
        self.c.execute(sql, tuple(args))
        self.dbc.commit()
        try:
            return self.c.fetchall()
        except psycopg2.ProgrammingError:
            return None

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
            WHERE job_id=%s
        """
        result = dbc.do(sql, job_id)
        return False if len(result) > 0 else True

    @classmethod
    def add_job(cls, dbc, job_id, job_title):
        """
        Add a new job to the database
        """
        sql = """
            INSERT INTO jobs (job_id, job_title) VALUES (%s, %s)
        """
        dbc.do(sql, job_id, job_title)
        logging.info('{0:%Y-%b-%d %H:%M:%S} Job {1} added'.format(datetime.now(), job_id))

    @classmethod
    def add_to_sublist(cls, dbc, chat_id, username=None, first_name=None,
                       last_name=None):
        """
        Add user to the sublist
        """
        sql = """
            INSERT INTO subscribers (chat_id, username, first_name, last_name)
                   VALUES (%s, %s, %s, %s)
        """
        return Connection.do(dbc, sql, chat_id, username, first_name, last_name)

    @classmethod
    def get_subs(cls, dbc, status=None):
        """
        Get all subscribers
        """
        sql = """
            SELECT *
            FROM subscribers
        """
        if status:
            sql += """
                WHERE status = %s
            """
            return Connection.do(dbc, sql, status)
        return Connection.do(dbc, sql)

    @classmethod
    def get_sub_one(cls, dbc, chat_id):
        """
        Get subscriber by his chat_id
        """
        sql = """
            SELECT *
            FROM subscribers
            WHERE chat_id = %s
        """
        return Connection.do(dbc, sql, chat_id)

    @classmethod
    def count_subs(cls, dbc):
        """
        Get the total number of subscribers
        """
        sql = """
            SELECT COUNT(*) as total
            FROM subscribers
        """
        return Connection.do(dbc, sql)

    @classmethod
    def set_subscription(cls, dbc, chat_id, subscription):
        """
        Set user's subscription status
        """
        sql = """
                UPDATE subscribers
                SET status = %s
                WHERE chat_id = %s
            """
        return Connection.do(dbc, sql, subscription, chat_id)


def parse_jobs(bot, job):
    """
    Parse the VUB student jobs website to find new posted jobs
    and push them to Telegram
    """
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
            subs = Query.get_subs(dbc, status='active')
            if subs:
                _count = 0
                for sub in subs:
                    "Push to Telegram"
                    bot.send_message(chat_id=sub['chat_id'],
                                     text='New job "{0}" is posted at {1}'.format(titles[i], url_job))
                    _count += 1
                    logging.info(
                        '{2:%Y-%b-%d %H:%M:%S} Notification about the job {0} is sent to {1} user(s)'.format(ids[i],
                                                                                                             _count,
                                                                                                             datetime.now()))
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
        user = update.message.from_user
        try:
            un = user['username']
        except KeyError:
            un = None
        try:
            fn = user['first_name']
        except KeyError:
            fn = None
        try:
            ln = user['last_name']
        except KeyError:
            ln = None
        Query.add_to_sublist(dbc, str(update.message.chat_id),
                             username=un,
                             first_name=fn,
                             last_name=ln)
        logging.info('{1:%Y-%b-%d %H:%M:%S} User {0} is added on the sublist.'.format(update.message.chat_id,
                                                                                      datetime.now()))
        bot.send_message(chat_id=update.message.chat_id,
                         text='You will be notified about newposted student '
                         'jobs at VUB.')
    elif on_sublist[0]['status'] == 'inactive':
        Query.set_subscription(dbc, update.message.chat_id, 'active')
        bot.send_message(chat_id=update.message.chat_id,
                         text='You will be notified about newposted student '
                              'jobs at VUB.')
        logging.info('{1:%Y-%b-%d %H:%M:%S} User {0} is added on the sublist.'.format(update.message.chat_id,
                                                                                      datetime.now()))
    else:
        bot.send_message(chat_id=update.message.chat_id,
                         text='You are already subscribed.')
    dbc.close()


def stop_com(bot, update):
    """
    COMMAND /stop
    Unsubscribe user from job updates
    """
    dbc = Connection()
    on_sublist = Query.get_sub_one(dbc, update.message.chat_id)
    if on_sublist and on_sublist[0]['status'] != 'inactive':
        Query.set_subscription(dbc, update.message.chat_id, 'inactive')
        logging.info('{1:%Y-%b-%d %H:%M:%S} User {0} is removed from the sublist.'.format(update.message.chat_id,
                                                                                          datetime.now()))
        bot.send_message(chat_id=update.message.chat_id,
                         text='You canceled your subscription successfully.'
                              '\nSend me /start to subscribe again.')
    else:
        bot.send_message(chat_id=update.message.chat_id, text='You are not subscribed.')
    dbc.close()


def help_com(bot, update):
    """
    COMMAND /help
    """
    bot.send_message(chat_id=update.message.chat_id,
                     text='Something helpful should be here.'
                          '\n/start — subscribe for updates'
                          '\n/stop – unsubscribe from updates'
                          '\n/help – obviously')


def sub_com(bot, update):
    """
    COMMAND /sub
    For admin's private usage. Get list and total number of subscribers
    """
    if update.message.chat_id == ID_ADMIN:
        dbc = Connection()
        subs = Query.get_subs(dbc)
        sub_list = ''
        for sub in subs:
            sub_list += str_dict(sub)
            sub_list += '\n'
        bot.send_message(chat_id=ID_ADMIN,
                         text=sub_list)
        total = Query.count_subs(dbc)[0]['total']
        bot.send_message(chat_id=ID_ADMIN,
                         text='Total: {0} sub(s).'.format(total))
        dbc.close()
    else:
        guest = update.message.from_user
        bot.send_message(chat_id=update.message.chat_id,
                         text='Access denied.')
        bot.send_message(chat_id=ID_ADMIN,
                         text='{0} tried to get subs'.format(guest))


def reply(bot, update):
    """
    Handle all non-command messages
    """
    bot.send_message(chat_id=update.message.chat_id,
                     text='Seems like it is not a command, I cannot understand you.'
                          '\nUse /help to get the full command-list.')


def str_dict(dictio):
    """
    Transform dict with sub's data to a better format for reading
    """
    result = ''
    keys = ['id', 'username', 'first_name', 'last_name', 'status']
    for key in keys:
        if key != 'chat_id' and dictio[key] not in ['', None]:
            try:
                result += '{0}: {1} '.format(key, dictio[key].upper())
            except AttributeError:
                result += '{0}: {1} '.format(key, dictio[key])
    return result


def main():
    updater = Updater(TOKEN)
    dp = updater.dispatcher
    j = updater.job_queue

    dp.add_handler(CommandHandler('start', start_com))
    dp.add_handler(CommandHandler('help', help_com))
    dp.add_handler(CommandHandler('stop', stop_com))
    dp.add_handler(CommandHandler('sub', sub_com))
    dp.add_handler(MessageHandler(Filters.text, reply))
    j.put(Job(parse_jobs, 60.0), next_t=0.00)

    updater.start_polling()
    updater.idle()


if __name__ == '__main__':
    main()
