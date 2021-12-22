import csv
import os

import pandas as pd


def compute_all():
    dir = os.path.abspath('./output')
    files = os.listdir(dir)
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
    active_commits_month = active.groupby(pd.Grouper(key='start_date', freq='M')).count()[
        ['contributor_login']]
    active_commits_month['protocol'] = df['org'][0]
    return active_commits_month


if __name__ == '__main__':
    compute_all()