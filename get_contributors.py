import csv
import json
import logging
import os
import time
from os import path
import optparse
from typing import List, Dict, Optional

import toml
from github import Github, StatsContributor
from joblib import Parallel, delayed
import requests

from config import get_pats
from gitTokenHelper import GithubPersonalAccessTokenHelper
from logger import sys


LOGGER = logging.getLogger(__file__)
dir_path = path.dirname(path.realpath(__file__))


class RepoStats:
    def __init__(self, pat: str, save_path: str):
        self.save_path = save_path
        self.gh_pat_helper = GithubPersonalAccessTokenHelper(get_pats())
        self.PAT = self._get_access_token()
        self.gh = Github(self.PAT)

    def _get_access_token(self):
        res = self.gh_pat_helper.get_access_token()
        if "token" in res and res["token"] is not None:
            return res["token"]
        print('Going to sleep since no token exists with usable rate limit')
        time.sleep(res["sleep_time_secs"])
        return self._get_access_token()

    def _get_with_retry(self, func, retry_num, **params):
        from github.GithubException import UnknownObjectException
        try:
            response = func(**params)

            if response.status_code == 403:
                if retry_num == 0:
                    LOGGER.info('retrying')
                    self.PAT = self._get_access_token()
                    self.gh = Github(self.PAT)
                    return self._get_with_retry(func, retry_num + 1, **params)
                else:
                    raise Exception('Rate limited')

            return response
        except UnknownObjectException as ex:
            LOGGER.error('object not found')
            raise ex
        except Exception as ex:
            LOGGER.exception(ex)
            raise ex

    def get_and_save_full_stats(self, chain_name: str):
        repos = self._read_repos_for_chain_from_toml(chain_name)
        print(f'Found {len(repos)} repos')
        repo_data_lists = Parallel(n_jobs=1)(delayed(
            self._get_commits)(chain, org_and_repo) for org_and_repo in repos)

        # org_repo_data_list = []
        # for org_url in repos:
        #     if not org_url.startswith("https://github.com/"):
        #         # TODO: If Gitlab repo then use Gitlab APIs
        #         print("%s is not a github repo...Skipping" % org_url)
        #         continue
        #     org = org_url.split("https://github.com/")[1]
        #     print("Fetching repo data for", org)
        #     org_repo_data = self._get_repo_data_for_org(chain_name, org)
        #     if len(org_repo_data) > 0:
        #         org_repo_data_list.append(org_repo_data)

        path = os.path.abspath("./output/" + chain + "_contributors.csv")

        with open(path, 'w+') as fileobj:
            writer = csv.DictWriter(fileobj, fieldnames=[
                'chain',
                'org',
                'repo',
                'commit_committer',
                'commit_author',
                'author',
                'committer'
            ])
            writer.writeheader()
            for repo_data in repo_data_lists:
                if len(repo_data) > 0:
                    writer.writerows(repo_data)

    # list all the repos of a github org/user
    # Ensure chain_name is same as name of toml file
    def _read_repos_for_chain_from_toml(self, chain: str):
        toml_file_path = path.join(dir_path, 'crypto-ecosystems/data/ecosystems', chain[0],
                                   chain + '.toml')
        if not path.exists(toml_file_path):
            print(".toml file not found for %s" % chain)
            sys.exit(1)
        try:
            with open(toml_file_path, 'r') as f:
                data = f.read()
            print("Fetching organizations for %s from toml file ..." % chain)
            repos = toml.loads(data)['repo']
            return [repo['url'].replace('https://github.com/', '') for repo in repos]
            # github_orgs = toml.loads(data)['github_organizations']
        except:
            print('Could not open toml file - check formatting.')
            sys.exit(1)

    # given the org_name, return list of organisation repos
    def _make_org_repo_list(self, org: str):
        org_repos = []
        entity = self._get_with_retry(self.gh.get_organization, 0, **{'login': org})
        if entity is None:
            print('organization not found, fetching user instead')
            entity = self._get_with_retry(self.gh.get_user, 0, **{'login': org})
            if entity is None:
                print('user not found, skipping repos')
                return org_repos

        for repo in entity.get_repos():
            org_repos.append(repo.name)
        org_repos = [org + '/{0}'.format(repo) for repo in org_repos]
        return org_repos

    def _get_repos_for_org(self, org):
        repos = []
        page = 1
        url = f"https://api.github.com/orgs/{org}/repos?type=forks&page={page}&per_page=1000"
        response = requests.get(
            url, headers={'Authorization': 'Token ' + self.PAT})
        while len(response.json()) > 0:
            for repo in response.json():
                repos.append(repo["full_name"])
            page += 1
            url = f"https://api.github.com/orgs/{org}/repos?type=forks&page={page}&per_page=1000"
            response = requests.get(
                url, headers={'Authorization': 'Token ' + self.PAT})

        return repos

    # get the data for all the repos of a github organization
    def _get_repo_data_for_org(self, chain: str, org: str):
        org_repos = self._make_org_repo_list(org)
        if len(org_repos) == 0:
            return []

        forked_repos = self._get_with_retry(self._get_repos_for_org, 0, **{'org': org})
        unforked_repos = list(set(org_repos) - set(forked_repos))
        # GitHub API can hit spam limit
        # number_of_hyperthreads = multiprocessing.cpu_count()
        number_of_hyperthreads = 1
        n_jobs = 2 if number_of_hyperthreads > 2 else number_of_hyperthreads
        print("Fetching single repo data ...")
        repo_data_lists = Parallel(n_jobs=n_jobs)(delayed(
            self._get_commits_with_retry)(chain, org_and_repo) for org_and_repo in unforked_repos)
        return repo_data_lists

    # Stats
    def _get_commits_with_retry(self, chain, org_and_repo):
        return self._get_with_retry(self._get_commits, 0, **{'chain': chain,
                                                             'org_and_repo': org_and_repo})

    def _get_commits(self, chain, org_and_repo) -> List[Dict]:
        page = 1
        commits = []

        def _get_commit_page(page):
            url = f"https://api.github.com/repos/{org_and_repo}/commits?page={page}&per_page=1000"
            return requests.get(url, headers={'Authorization': 'Token ' + self.PAT})

        response = self._get_with_retry(_get_commit_page, 0, **{'page': page})

        while response.status_code == 200 and len(response.json()) > 0:
            for commit in response.json():
                commits.append({
                    'chain': chain,
                    'org': org_and_repo.split('/')[0],
                    'repo': org_and_repo.split('/')[1],
                    'commit_committer': json.dumps(commit['commit']['committer']),
                    'commit_author': json.dumps(commit['commit']['author']),
                    'author': json.dumps(commit['author']),
                    'committer': json.dumps(commit['committer'])
                })
            page += 1

            response = self._get_with_retry(_get_commit_page, 0, **{'page': page})

        print(f'Fetched {len(commits)} commits for {org_and_repo}')
        return commits

    # get repo data using a repo URL in the form of `org/repo`
    def _get_single_repo_data_from_api(self, chain: str, org: str, org_then_slash_then_repo: str,
                                       year_count: int = 1):
        print('Fetching repo data for ', org_then_slash_then_repo)
        data = []
        try:
            repo = self.gh.get_repo(org_then_slash_then_repo)
            contributors: Optional[List[StatsContributor]] = repo.get_stats_contributors()
            if contributors is None:
                return data

            for contributor in contributors:
                for week in contributor.weeks:
                    if week.c > 0:
                        data.append({
                            'chain': chain,
                            'org': org,
                            'repo': org_then_slash_then_repo.split("/")[1],
                            'contributor_login': contributor.author.login,
                            'contributor_id': contributor.author.id,
                            'start_date': week.w.strftime('%Y-%m-%dT%H:%M:%S%zZ'),
                            'additions': week.a,
                            'deletions': week.d,
                            'commits': week.c
                        })
            return data

        except Exception as e:
            LOGGER.exception(e)
            self.PAT = self._get_access_token()
            self.gh = Github(self.PAT)
            return self._get_single_repo_data_from_api(chain, org, org_then_slash_then_repo,
                                                       year_count)


if __name__ == '__main__':
    p = optparse.OptionParser()
    p.add_option('--frequency', type='int', dest='frequency',
                 help='Enter churn, commit frequency')

    options, arguments = p.parse_args()

    years_count = int(sys.argv[2]) if len(sys.argv) > 2 else 3

    pat: str = os.getenv('GITHUB_PAT')
    do = RepoStats(pat=pat, save_path='./output')
    chains = os.getenv('CHAINS').split(" ")
    for chain in chains:
        print('getting stats for chain', chain)
        do.get_and_save_full_stats(chain)
