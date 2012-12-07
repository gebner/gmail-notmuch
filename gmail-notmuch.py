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


# $ ./gmail-notmuch.py -u jason.donenfeld -p Shien8Boh2vah ~/Mail/
# Logging in...
# Selecting all mail...
# Collecting old messages...
# Reading message list...
# Downloading messages: 44 of 200|########                              | 22% ETA:  0:00:58 Elapsed Time: 0:00:16   2.66  emails/s

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
	parser = OptionParser(usage="%prog --username/-u USERNAME --password/-p PASSWORD --verbose/-v MAILDIR", description="Slurps gmail messages with labels into a notmuch maildir.")
	parser.add_option("-u", "--username", action="store", type="string", metavar="USERNAME", help="Gmail username")
	parser.add_option("-p", "--password", action="store", type="string", metavar="PASSWORD", help="Gmail password")
	parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False, help="Verbose output")
	(options, args) = parser.parse_args()
	if options.username == None or options.password == None:
		parser.error("Username and password are required.")
	if not options.username.lower().endswith("@gmail.com") and not options.username.lower().endswith("@googlemail.com"):
		options.username += "@gmail.com"
	if len(args) == 0:
		parser.error("Maildir location is required.")

	destination = os.path.abspath(args[0])

	imap = login(options)
	database = notmuch.Database(destination, False, notmuch.Database.MODE.READ_WRITE)

	messages = discover_messages(imap)
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
		imap = login(options)

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
	imap.select("[Gmail]/All Mail", True)
	return imap

def discover_messages(imap):
	print("Receiving message list and labels (this may take a while)...")
	parser = re.compile(r'([0-9]+) [(]X-GM-MSGID ([0-9]+) X-GM-LABELS [(](.*)[)] FLAGS [(](.*)[)][)]')
	typ, data = imap.fetch("1:*", "(FLAGS X-GM-LABELS X-GM-MSGID)")
	new_messages = []
	if typ != "OK":
		sys.exit(("Failed to discover new messages: %s" % typ))
	print("Parsing message list and labels...")
	for response in data:
		imap_seq, gmail_id, labels, flags = parser.search(response).groups()
		labels = filter_labels(shlex.split(labels, False, True) + flags.split(" "))
		new_messages.append((gmail_id, imap_seq, labels))
	return new_messages

def tag_message(database, filename, labels):
	database.begin_atomic()
	message = database.add_message(filename, False)[0]
	try:
		message.freeze()
		message.remove_all_tags(False)
		for tag in labels:
			message.add_tag(tag, False)
		message.thaw()
		database.end_atomic()
		message.tags_to_maildir_flags()
	except Exception as e:
		database.remove_message(message)
		database.end_atomic()
		raise e

def retag_old_messages(database, messages, destination):
	print("Searching for local messages...")
	old_messages = { os.path.basename(filename[0:filename.rfind(".gmail")]): destination + "/cur/" + filename for filename in os.listdir(destination + "/cur/") if ".gmail" in filename }
	new_messages = []
	i = 1
	progressbar = ProgressBar(maxval=len(old_messages), widgets=["Retagging local messages: ", SimpleProgress(), Bar(), Percentage(), " ", ETA(), " ", Timer(), " ", FileTransferSpeed(unit="emails")])
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
	progressbar = ProgressBar(maxval=len(messages), widgets=["Downloading messages: ", SimpleProgress(), Bar(), Percentage(), " ", ETA(), " ", Timer(), " ", FileTransferSpeed(unit="emails")])
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
		sys.exit(1)
