import argparse
import base64
import json
import re
from os.path import normpath, exists
from datetime import datetime
from sys import exit as s_exit
from ast import literal_eval
from ets.ets_mysql_lib import MysqlConnection as Mc
from transliterate import translit

PROGNAME = 'Mysql query Nagios plugin'
DESCRIPTION = '''Плагин Nagios для выполнения запросов (с возможностью сброса ошибок по дате)'''
VERSION = '1.0'
AUTHOR = 'Belim S.'
RELEASE_DATE = '2018-06-20'

OK, WARNING, CRITICAL, UNKNOWN = range(4)

DEFAULT_DATA_FILE = 'data.json'
DEFAULT_DATA_SEPARATOR = ' | '
DEFAULT_DATE_SUB_STRING = 'MONITORING_DATE_TIME'
DEFAULT_WARNING_LIMIT = 1
DEFAULT_CRITICAL_LIMIT = 1

date_format = '%Y-%m-%d %H:%M:%S'
error_text = 'Found %s new errors:'
no_error_text = 'Errors not found'


# обработчик параметров командной строки
def create_parser():
    parser = argparse.ArgumentParser(description=DESCRIPTION)

    parser.add_argument('-v', '--version', action='store_true',
                        help="Показать версию программы")

    parser.add_argument('-f', '--data_file', type=str, default=DEFAULT_DATA_FILE,
                        help="Файл данных (по умолчанию %s)" % DEFAULT_DATA_FILE)

    parser.add_argument('-p', '--data_separator', type=str, default=DEFAULT_DATA_SEPARATOR,
                        help="Разделитель вывода данных(по умолчанию '%s')" % DEFAULT_DATA_SEPARATOR)

    parser.add_argument('-n', '--nagios_name', type=str,
                        help="Метка метрики в файле данных")

    parser.add_argument('-i', '--connection', type=str,
                        help="Используемое подключение к БД")

    parser.add_argument('-w', '--warning_limit', type=int, default=DEFAULT_WARNING_LIMIT,
                        help="Лимит для срабатывания WARNING (по умолчанию %s)" % DEFAULT_WARNING_LIMIT)

    parser.add_argument('-c', '--critical_limit', type=int, default=DEFAULT_CRITICAL_LIMIT,
                        help="Лимит для срабатывания CRITICAL (по умолчанию %s)" % DEFAULT_CRITICAL_LIMIT)

    parser.add_argument('-q', '--query', type=str,
                        help="Запрос закодированный в base64")

    parser.add_argument('-b', '--date_sub_string', type=str, default=DEFAULT_DATE_SUB_STRING,
                        help="Подстрока c датой используемая для замены в запросе (по умолчанию '%s')" %
                             DEFAULT_DATE_SUB_STRING)

    parser.add_argument('-r', '--drop_errors', action='store_true',
                        help="Сброс ошибок")

    parser.add_argument('-s', '--show_connections', action='store_true',
                        help="Вывести список доступных подключений")

    parser.add_argument('-m', '--show_nagios_names', action='store_true',
                        help="Вывести список доступных меток метрик из файла")

    return parser


def show_version():
    print(PROGNAME, VERSION, '\n', DESCRIPTION, '\nAuthor:', AUTHOR, '\nRelease date:', RELEASE_DATE)


def get_datetime(j_file, n_name, update=False):
    """Функция получения времени из файла
    j_file -- файл данных
    n_name -- метка нагиоса
    update -- если True - обновить данные в файле
    """
    if not exists(j_file):
        json_loads_data = {}
        with open(j_file, mode='w') as tmp_json_f:
            tmp_json_f.write(json.dumps(json_loads_data))
    else:
        with open(j_file, mode='r') as tmp_json_f:
            tmp_json_r = tmp_json_f.read()
        json_loads_data = json.loads(tmp_json_r)

    if (n_name not in json_loads_data) or update:
        actual_datetime = datetime.now().strftime(date_format)
        json_loads_data[n_name] = actual_datetime
        with open(j_file, mode='w') as tmp_json_f:
            tmp_json_f.write(json.dumps(json_loads_data))
    else:
        actual_datetime = json_loads_data[n_name]
    return actual_datetime


def show_nagios_nm(j_file):
    """Функция вывода меток из файла
    j_file -- файл данных
    """
    if not exists(j_file):
        json_loads_data = {}
        with open(j_file, mode='w') as tmp_json_f:
            tmp_json_f.write(json.dumps(json_loads_data))
    else:
        with open(j_file, mode='r') as tmp_json_f:
            tmp_json_r = tmp_json_f.read()
        json_loads_data = json.loads(tmp_json_r)

    print('Available nagios names:')
    for name, dtm in json_loads_data.items():
        print('%s (%s)' % (name, dtm))


def get_available_connections():
    """Функция получения доступных подключений mysql"""
    m = Mc()
    return sorted([cn for cn in m.__dir__() if cn.startswith('MS')])


def show_connects():
    print('Available connections:')
    for connect in get_available_connections():
        print(connect)


def get_query(string):
    """Функция получения запросы из base64"""
    byte_str = literal_eval('b\'' + string + '\'')
    txt_str = base64.b64decode(byte_str).decode("utf-8")
    return txt_str


def get_connection(connection_name):
    """Получение подключения"""
    if connection_name not in get_available_connections():
        print('Unknown connection %s' % connection_name)
        s_exit(UNKNOWN)

    return Mc().__getattribute__(connection_name)


def check_arguments(args):
    for arg in args:
        if globals()[arg] is None:
            print('Argument %s not set' % arg)
            s_exit(UNKNOWN)

if __name__ == '__main__':
    try:
        # парсим аргументы командной строки
        my_parser = create_parser()
        namespace = my_parser.parse_args()

        data_file = normpath(namespace.data_file)
        data_separator = namespace.data_separator
        nagios_name = namespace.nagios_name
        connection = namespace.connection
        warning_limit = namespace.warning_limit
        critical_limit = namespace.critical_limit
        query = namespace.query
        date_sub_string = namespace.date_sub_string

        # вывод версии
        if namespace.version:
            show_version()
            s_exit(OK)

        # вывод доступных подключений
        if namespace.show_connections:
            show_connects()
            s_exit(OK)

        # если установлен флаг сброса, до обновляем данные в файле
        if namespace.drop_errors:

            check_arguments(('nagios_name', 'data_file'))

            get_datetime(data_file, nagios_name, update=True)
            print('Error drop successfully!')
            s_exit(OK)

        # вывод списка доступных меток нагиос
        if namespace.show_nagios_names:
            if not exists(data_file):
                print('Datafile %s not found' % data_file)
                s_exit(UNKNOWN)

            show_nagios_nm(data_file)
            s_exit(OK)

        # если указан запрос, то выполняем его
        if namespace.query:

            check_arguments(('nagios_name', 'connection'))

            last_update_datetime = get_datetime(data_file, nagios_name)
            query = re.sub(date_sub_string, '\'' + last_update_datetime + '\'', get_query(query))
            connection = get_connection(connection)

            # получаем данные по запросу
            mc = Mc(connection=connection)
            with mc.open():
                query_data = mc.execute_query(query)

            # преобразовываем все данные в строки
            out_info = tuple(map(lambda x: [str(line) for line in x], query_data))

            # если
            if out_info:
                out_info_len = len(out_info)

                print(error_text % out_info_len)
                for info_line in out_info:
                    print(translit(str(data_separator.join(info_line)), 'ru', reversed=True))

                if out_info_len >= critical_limit:
                    s_exit(CRITICAL)

                elif out_info_len >= warning_limit:
                    s_exit(WARNING)
            else:
                print(no_error_text)
                s_exit(OK)

    except Exception as err:
        print('Plugin error')
        print(err)
        s_exit(UNKNOWN)

    show_version()
    print('For more information run use --help')
    s_exit(UNKNOWN)
