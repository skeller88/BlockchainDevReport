import csv
import logging
import os
import time
from os import path
import optparse

import toml
from github import Github
from joblib import Parallel, delayed
import requests

from logger import sys
from gitTokenHelper import GithubPersonalAccessTokenHelper
from config import get_pats


LOGGER = logging.getLogger(__file__)
dir_path = path.dirname(path.realpath(__file__))


class RepoStats:
    def __init__(self, save_path: str, frequency):
        self.save_path = save_path
        self.gh_pat_helper = GithubPersonalAccessTokenHelper(get_pats())
        self.PAT = self._get_access_token()
        self.gh = Github(self.PAT)
        # churn, commit frequency
        self.frequency = frequency

    def _get_access_token(self):
        res = self.gh_pat_helper.get_access_token()
        if "token" in res and res["token"] is not None:
            return res["token"]
        print('Going to sleep since no token exists with usable rate limit')
        time.sleep(res["sleep_time_secs"])
        return self._get_access_token()

    def get_and_save_full_stats(self, chain_name: str, year_count):
        github_orgs = self._read_orgs_for_chain_from_toml(chain_name)

        for org_url in github_orgs:
            if not org_url.startswith("https://github.com/"):
                # TODO: If Gitlab repo then use Gitlab APIs
                print("%s is not a github repo...Skipping" % org_url)
                continue
            org = org_url.split("https://github.com/")[1]
            print("Fetching repo data for", org)
            org_repo_data_list = self._get_repo_data_for_org(org, year_count)

    # list all the repos of a github org/user
    # Ensure chain_name is same as name of toml file
    def _read_orgs_for_chain_from_toml(self, chain_name):
        toml_file_path = path.join(dir_path, 'protocols', chain_name + '.toml')
        if not path.exists(toml_file_path):
            print(".toml file not found for %s in /protocols folder" % chain_name)
            sys.exit(1)
        try:
            with open(toml_file_path, 'r') as f:
                data = f.read()
            print("Fetching organizations for %s from toml file ..." % chain_name)
            github_orgs = toml.loads(data)['github_organizations']
            return github_orgs
        except:
            print('Could not open toml file - check formatting.')
            sys.exit(1)

    # given the org_name, return list of organisation repos
    def _make_org_repo_list(self, org_name: str):
        org_repos = []
        try:
            entity = self.gh.get_organization(org_name)
        except:
            entity = self.gh.get_user(org_name)
        for repo in entity.get_repos():
            org_repos.append(repo.name)
        org_repos = [org_name + '/{0}'.format(repo) for repo in org_repos]
        return org_repos

    # get the data for all the repos of a github organization
    def _get_repo_data_for_org(self, org_name: str, year_count=1):
        org_repos = self._make_org_repo_list(org_name)
        forked_repos = []
        page = 1
        url = f"https://api.github.com/orgs/{org_name}/repos?type=forks&page={page}&per_page=100"
        response = requests.get(
            url, headers={'Authorization': 'Token ' + self.PAT})
        while len(response.json()) > 0:
            for repo in response.json():
                forked_repos.append(repo["full_name"])
            page += 1
            url = f"https://api.github.com/orgs/{org_name}/repos?type=forks&page={page}&per_page=100"
            response = requests.get(
                url, headers={'Authorization': 'Token ' + self.PAT})
        unforked_repos = list(set(org_repos) - set(forked_repos))
        # GitHub API can hit spam limit
        # number_of_hyperthreads = multiprocessing.cpu_count()
        number_of_hyperthreads = 1
        n_jobs = 2 if number_of_hyperthreads > 2 else number_of_hyperthreads
        print("Fetching single repo data ...")
        repo_data_lists = Parallel(n_jobs=n_jobs)(delayed(
            self._get_single_repo_data_from_api)(org_name, repo, year_count) for repo in unforked_repos)

        path = os.path.abspath("./output/" + org_name + "_single_repo_stats.csv")

        with open(path, 'w+') as single_repo_data:
            writer = csv.DictWriter(single_repo_data, fieldnames=[
                'org', 'repo', 'contributor_login', 'contributor_id', 'start_date', 'additions',
                'deletions', 'commits'])
            for repo_data in repo_data_lists:
                writer.writerows(repo_data)

    # Stats
    # get repo data using a repo URL in the form of `org/repo`
    def _get_single_repo_data_from_api(self, org: str, org_then_slash_then_repo: str, year_count: int = 1):
        print('Fetching repo data for ', org_then_slash_then_repo)
        data = []
        try:
            repo = self.gh.get_repo(org_then_slash_then_repo)
            for contributor in repo.get_stats_contributors():
                for week in contributor.weeks:
                    data.append({
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
            print("Token rate limit reached, switching tokens")
            PAT = self._get_access_token()
            self.gh = Github(PAT)
            return self._get_single_repo_data_from_api(org, org_then_slash_then_repo, year_count)
            raise e


if __name__ == '__main__':
    p = optparse.OptionParser()
    p.add_option('--frequency', type='int', dest='frequency',
                 help='Enter churn, commit frequency')

    options, arguments = p.parse_args()
    if not options.frequency:
        options.frequency = 4

    years_count = int(sys.argv[2]) if len(sys.argv) > 2 else 1

    do = RepoStats('./output', options.frequency)
    # do.get_and_save_full_stats(sys.argv[1], years_count)
    do.get_and_save_full_stats('algorand', years_count)


