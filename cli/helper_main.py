# -*- encoding: utf-8 -*-

import ConfigParser
import logging
import logging.config
import os
import sys
import shutil
import threading
import traceback
import time

import git

from common import toolbar_lib

logging.config.fileConfig('etc/helper_logging.conf')

class GitChecker(object):
    """check git repo"""
    def __init__(self, git_url, git_branch, git_cache, git_dest, git_check_interval):
        self.git_url = git_url
        self.git_branch = git_branch
        self.git_cache = git_cache
        self.git_dest = git_dest
        self.git_check_interval = git_check_interval
        self.git_repo = None
        self.changed = []

        self.update_event = threading.Event()

        if not os.path.exists(self.git_cache):
            os.makedirs(self.git_cache)
            self.git_repo = git.Repo.clone_from(self.git_url, self.git_cache,
                                                branch=self.git_branch)
        else:
            self.git_repo = git.Repo(path=self.git_cache)

    def check(self):
        """check git repo"""
        while True:
            try:
                self.update_event.wait(self.git_check_interval)
                remote = self.git_repo.remote("origin")
                fetch_result = remote.fetch("--dry-run")

                # if there is no update, the old_commit should be None
                if fetch_result[0].old_commit is None:
                    self.update_event.clear()
                    continue

                pull_result = remote.pull()
                changed_files = [ item.a_path for item in self.git_repo.index.diff(pull_result[0].old_commit) ]
                if changed_files:
                    self.copy_file_2_dest(changed_files)

                self.update_event.clear()
            except:
                logging.error(traceback.format_exc())

    def copy_file_2_dest(self, changedFiles):
        for f in changedFiles:
            shutil.copy2(f, self.git_dest)

    def update_source(self):
        self.update_event.set()

    def run(self):
        """start the gitchecker"""
        threadobj = threading.Thread(target=self.check)
        threadobj.daemon = True
        threadobj.start()


def main():
    """helper"""
    #read command params
    config_file = toolbar_lib.check_para(sys.argv, "f", "etc/helper.ini")
    #read config
    config = ConfigParser.ConfigParser()
    config.read(config_file)

    git_url = config.get("git", "url")
    git_branch = config.get("git", "branch")
    git_cache = config.get("git", "cache")
    git_dest = config.get("git", "dest")
    git_check_interval = config.getint("git", "check_interval")

    git_checker = GitChecker(git_url, git_branch, git_cache, git_dest,
                             git_check_interval)
    git_checker.run()

    time.sleep(5)


if __name__ == "__main__":
    main()
