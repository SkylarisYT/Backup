import logging
from github import Github, InputGitTreeElement
from subprocess import Popen, PIPE
from os import getcwd, system, listdir, environ
from time import sleep

cwd = getcwd()

access_token = environ.get("ACCESS_TOKEN")
repo_link = environ.get("REPO")
rethinkdb_pw = environ.get("RETHINKDB_PW") != "0" and environ.get("RETHINKDB_PW")
dump_name = environ.get("DUMP_NAME") != "0" and environ.get("DUMP_NAME")
host = environ.get("HOST", "localhost:28015")

g = Github(access_token)

logging.basicConfig(level=logging.INFO)

def get_new_dump():
	"""Dumps the RethinkDB database and returns the name of the dump."""

	options = ["python", "-mrethinkdb._dump", "-c", host]

	if dump_name:
		options.append("-f")
		options.append("%s.tar.gz" % (dump_name))

	if rethinkdb_pw:
		options.append("-p")

		p = Popen(options, stdin=PIPE, stdout=PIPE, stderr=PIPE)
		p.communicate(input=bytes("%s\n" % (rethinkdb_pw), "utf-8"))

	else:
		p = Popen(options, stdin=PIPE, stdout=PIPE, stderr=PIPE)
		p.wait()

	if dump_name:
		return "%s.tar.gz" % (dump_name)

	files = [x for x in listdir() if x.startswith("rethinkdb_dump")]
	if files:
		return files[len(files) - 1]

def clean_dumps(current_name=None):
	"""removes all rethinkdb dumps"""

	for file in listdir():
		if file.endswith(".tar.gz"):
			if file == current_name:
				system("rm %s" % (current_name))
			else:
				if file.startswith("rethinkdb_dump"):
					system("rm %s" % (file))

def new_backup():
	"""Dumps the database and uploads it to GitHub."""

	repo = g.get_repo(repo_link)
	location = get_new_dump()

	master_ref = repo.get_git_ref("heads/master")
	master_sha = master_ref.object.sha
	base_tree = repo.get_git_tree(master_sha)

	with open(location, "rb") as f:
		data = str(f.read())
		element = InputGitTreeElement(location, "100644", "blob", data)
		tree = repo.create_git_tree([element], base_tree)
		parent = repo.get_git_commit(master_sha)
		commit = repo.create_git_commit("Upload Backup", tree, [parent])
		master_ref.edit(commit.sha)

	clean_dumps(location)

def start_backups():
	while True:
		new_backup()
		sleep(86400)


try:
	start_backups()
finally:
	clean_dumps()
