import time


def print_sleep(interval):
    print('''
                    ------------------------------------
                      Done & Sleeping for {} seconds
                    ------------------------------------'''.format(interval))


def print_error_sleep(Exeception, interval):
    print('''
                    ------------------------------------
                      Come front: {}
                      Sleeping for {} seconds
                    ------------------------------------'''.format(Exeception, interval))

def print_error_pair(Error, pair, interval):
    print('''
                    ------------------------------------
                      Come front: {}
                      pair: {}
                      Sleeping for {} seconds
                    ------------------------------------'''.format(Error, pair, interval))

def print_write_data():
    print('''
                    ####################################
                      Time: {}
                      Writing data into DB....
                    ####################################'''.format(
                        time.asctime( time.localtime(time.time()))
                        ))