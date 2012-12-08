#!/usr/bin/env python

# (C) Copyright 2012 Jason A. Donenfeld <Jason@zx2c4.com>. All Rights Reserved.

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

# ==============================
# == Gmail-->Notmuch Importer ==
# ==                          ==
# ==     Work in progress.    ==
# ==         by zx2c4         ==
# ==                          ==
# ==============================


# $ ./gmail-notmuch.py -u jason.donenfeld -p Shien8Boh2vah
# Logging in...
# Selecting all mail...
# Receiving message list: 135126 of 135126|##################################################|100% Time: 0:00:52   2.56 kemails/s
# Parsing message list and labels...
# Searching for local messages...
# Retagging local messages: 135124 of 135124|################################################|100% Time: 0:00:13  10.39 kemails/s
# Downloading messages: 2 of 2|##############################################################|100% Time: 0:00:00   5.12  emails/s

# Interrupted imports will automatically resume from where they left off.

from imaplib import IMAP4_SSL
from optparse import OptionParser
import sys
import os.path
import os
import shlex
import re
import notmuch
from progressbar import *

def main():
	parser = OptionParser(usage="%prog --username/-u USERNAME --password/-p PASSWORD --verbose/-v", description="Slurps gmail messages with labels into a notmuch maildir.")
	parser.add_option("-u", "--username", action="store", type="string", metavar="USERNAME", help="Gmail username")
	parser.add_option("-p", "--password", action="store", type="string", metavar="PASSWORD", help="Gmail password")
	parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False, help="Verbose output")
	(options, args) = parser.parse_args()
	if options.username is None or options.password is None:
		parser.error("Username and password are required.")
	if "@" not in options.username:
		options.username += "@gmail.com"

	try:
		# Create should be True, but there's a bug at the moment.
		database = notmuch.Database(None, False, notmuch.Database.MODE.READ_WRITE)
	except notmuch.NotmuchError as e:
		print(str(e))
		sys.exit("You must create the notmuch database before running this program.")
	if database.needs_upgrade():
		database.upgrade()
	destination = database.get_path()
	for directory in ["cur", "new", "tmp"]:
		try:
			os.mkdir(destination + "/" + directory, 0770)
		except:
			pass

	imap, total = login(options)

	messages = discover_messages(imap, total)
	if len(messages) == 0:
		print("Discovered no messages!")
		logout(imap)
		sys.exit(0)
	
	new_messages = retag_old_messages(database, messages, destination)
	if len(new_messages) == 0:
		print("Discovered no new messages!")
		logout(imap)
		sys.exit(0)

	try:
		imap.noop()
	except IMAP4_SSL.abort:
		print("Server disconnected us.")
		imap, total = login(options)

	download_new_messages(imap, database, new_messages, destination)

	database.close()
	logout(imap)

def login(options):
	print("Logging in...")
	imap = IMAP4_SSL("imap.gmail.com")
	if options.verbose:
		imap.debug = 10
	imap.login(options.username, options.password)
	print("Selecting all mail...")
	typ, data = imap.xatom("XLIST", "", "*")
	if typ != "OK":
		sys.exit("Could not discover all mail.")
	allmail = None
	for label in imap.untagged_responses["XLIST"]:
		if b"\\AllMail" in label:
			last_quote = label.rfind("\"")
			penultimate_quote = label.rfind("\"", 0, last_quote) + 1
			allmail = label[penultimate_quote:last_quote]
	if allmail is None:
		sys.exit("Could not parse all mail.")
	typ, data = imap.select("\"" + allmail + "\"", True)
	if typ != "OK":
		sys.exit("Could not select all mail.")
	return imap, int(data[0])

def discover_messages(imap, total):
	parser = re.compile(r'([0-9]+) [(]X-GM-MSGID ([0-9]+) X-GM-LABELS [(](.*)[)] FLAGS [(](.*)[)][)]')

	old_readline = imap.readline
	def new_readline(self):
		ret = old_readline()
		if b"FETCH (X-GM-MSGID " in ret:
			new_readline.progressbar.update(new_readline.i)
			new_readline.i += 1
		return ret
	new_readline.i = 1
	new_readline.progressbar = create_progressbar("Receiving message list", total).start()
	imap.readline = new_readline.__get__(imap, imap.__class__)

	typ, data = imap.fetch("1:*", "(FLAGS X-GM-LABELS X-GM-MSGID)")
	new_readline.progressbar.finish()
	imap.readline = old_readline
	new_messages = []
	if typ != "OK":
		sys.exit("Failed to discover new messages: %s" % typ)

	print("Parsing message list and labels...")
	for response in data:
		imap_seq, gmail_id, labels, flags = parser.search(str(response)).groups()
		labels = filter_labels(shlex.split(labels, False, True) + flags.split(" "))
		new_messages.append((gmail_id, imap_seq, labels))
	return new_messages

def tag_message(database, filename, labels):
	message = None
	try:
		message = database.find_message_by_filename(filename)
		if message is None:
			database.begin_atomic()
			message = database.add_message(filename, False)[0]
		else:
			if set(labels) == set(message.get_tags()):
				message.tags_to_maildir_flags()
				return
			database.begin_atomic()
		message.freeze()
		message.remove_all_tags(False)
		for tag in labels:
			message.add_tag(tag, False)
		message.thaw()
		database.end_atomic()
		message.tags_to_maildir_flags()
	except Exception as e:
		if message is not None:
			database.remove_message(message)
		database.end_atomic()
		raise e

def create_progressbar(text, total):
	return ProgressBar(maxval=total, widgets=[text + ": ", SimpleProgress(), Bar(), Percentage(), " ", ETA(), " ", FileTransferSpeed(unit="emails")])

def retag_old_messages(database, messages, destination):
	print("Searching for local messages...")
	old_messages = { os.path.basename(filename[0:filename.rfind(".gmail")]): destination + "/cur/" + filename for filename in os.listdir(destination + "/cur/") if ".gmail" in filename }
	new_messages = []
	i = 1
	progressbar = create_progressbar("Retagging local messages", len(old_messages))
	progressbar.start()
	for gmail_id, imap_seq, labels in messages:
		if gmail_id in old_messages:
			tag_message(database, old_messages[gmail_id], labels)
			progressbar.update(i)
			i += 1
		else:
			new_messages.append((gmail_id, imap_seq, labels))
	progressbar.finish()
	return new_messages

def download_new_messages(imap, database, messages, destination):
	i = 1
	progressbar = create_progressbar("Downloading messages", len(messages))
	progressbar.start()
	for gmail_id, imap_seq, labels in messages:
		temp = destination + "/tmp/" + str(gmail_id) + ".gmail"
		dest = destination + "/new/" + str(gmail_id) + ".gmail"
		if not os.path.exists(dest):
			typ, data = imap.fetch(str(imap_seq), "RFC822")
			if typ != "OK":
				sys.exit("Failed to download message gmail-%d/imap-%d" % (gmail_id, imap_seq))
			f = open(temp, "w")
			f.write(data[0][1])
			f.close()
			os.link(temp, dest) # Because DJB says so...
			os.unlink(temp)
		tag_message(database, dest, labels)
		progressbar.update(i)
		i += 1
	progressbar.finish()

def filter_labels(labels):
	translation = {	"\\Inbox":	"inbox",
			"\\Drafts":	"draft",
			"\\Sent":	"sent",
			"\\Spam":	"spam",
			"\\Starred":	"flagged",
			"\\Trash":	"deleted",
			"\\Answered":	"replied",
			"\\Flagged":	"flagged",
			"\\Draft":	"draft",
			"\\Deleted":	"deleted",
			"\\Seen":	"!read!",
			"\\Important":	None, # I realize this is controversial, but I hate the priority inbox.
			"\\Muted":	None, # I also don't intend to use the muted idea going forward.
			"Junk":		"spam",
			"NonJunk":	None }
	ret = set()
	for label in labels:
		if label in translation:
			if translation[label] is None:
				continue
			ret.add(translation[label])
		else:
			ret.add(label)
	if "!read!" in ret:
		ret.remove("!read!")
	else:
		ret.add("unread")
	if "" in ret:
		ret.remove("")
	return ret

def logout(imap):
	imap.close()
	imap.logout()

if __name__ == '__main__':
	try:
		main()
	except KeyboardInterrupt:
		print("")
		sys.exit(1)
