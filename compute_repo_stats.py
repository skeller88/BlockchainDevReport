import csv
import os

import pandas as pd


def compute_all():
    dir = os.path.abspath('./output')
    files = os.listdir(dir)

    # date_range = pd.date_range(start='2017-01-01', end='2021-12-22', freq='M', tz='UTC',
    #                            name='index')
    # pd.DataFrame(date_range).set_index('index')

    for file in files:
       compute_for_protocol(file)


def compute_for_protocol(file):
    df = pd.read_csv(file)
        # names=[
        #     'chain', 'org', 'repo', 'contributor_login', 'contributor_id', 'start_date', 'additions',
        #     'deletions', 'commits'])
    df['start_date'] = pd.to_datetime(df['start_date'])
    date_range = pd.date_range(start=df['start_date'].min(), end=df['start_date'].max(), freq='M')

    active = df[df['commits'] > 0]
    active_contributors_month = active.groupby(pd.Grouper(key='start_date', freq='M')).count()[
        ['contributor_login']]
    active_contributors_month['chain'] = df['org'][0]

    active.groupby('contributor_login').sum()[['commits']].sort_values(by='commits',
                                                                       ascending=False)[:10]
    return active_contributors_month


def aggregate_protocols():
    dir = os.path.abspath('./output')
    files = os.listdir(dir)
    dfs = []
    for file in files:
       dfs.append(pd.read_csv(file))
    df = pd.concat(dfs)
    df.to_csv(dir + '/all_contributors.csv')

if __name__ == '__main__':
    compute_all()