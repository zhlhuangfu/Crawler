import time


def print_sleep(interval):
    print('''
                    ------------------------------------
                      Done & Sleeping for {} seconds
                    ------------------------------------'''.format(interval))


def print_error_sleep(interval):
    print('''
                    ------------------------------------
                      Come front with network error
                      Sleeping for {} seconds
                    ------------------------------------'''.format(interval))


def print_write_data():
    print('''
                    ####################################
                      Time: {}
                      Writing data into DB....
                    ####################################'''.format(
                        time.asctime( time.localtime(time.time()))
                        ))