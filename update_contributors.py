#!/usr/bin/env python
import json
import glob
import os
import requests

try:
    import urllib.parse
except ImportError:
    import urllib

def getUsers():
    with open('users.json') as f:
        return json.load(f)

def findFiles(ext=".md"):
    subpaths = [os.path.join('.github', "ISSUE_TEMPLATE"), '.gitlab']
    if not ext.startswith('.'):
        ext = '.' + ext
    files = glob.glob("*" + ext)
    for subpath in subpaths:
        tFiles = glob.glob(os.path.join(subpath,"*" + ext))
        files = files + tFiles
    return files

def checkurl(url):
    request = requests.get(url)
    return request.status_code == 200

def buildTable(realUsers):
    baseUrl = "https://www.openstreetmap.org/user/"
    nameMax = 0
    userNameMax = 0
    userNameEncMax = 0
    for user in realUsers:
        name = user['name']
        username = user['username']
        try:
            usernameEnc = urllib.parse.quote(username)
        except AttributeError:
            usernameEnc = urllib.quote(username)
        if 'comment' in user:
            name += " (" + user['comment'] + ")"
        if len(name) > nameMax:
            nameMax = len(name)
        if len(username) > userNameMax:
            userNameMax = len(username)
        if len(usernameEnc) > userNameEncMax:
            userNameEncMax = len(usernameEnc)
    table = ["| Name" + ' ' * (nameMax - len('Name')) + " | OSM Username" + ' ' * (len(baseUrl) + userNameMax + userNameEncMax + 4 - len('OSM Username')) + " |"]
    table.append("|" + "-" * (nameMax + 2) + "|" + "-" * (len(baseUrl) + userNameMax + userNameEncMax + 4 + 2) + "|")
    for user in realUsers:
        name = user['name']
        username = user['username']
        if 'comment' in user:
            name += " (" + user['comment'] + ")"
        try:
            usernameEnc = urllib.parse.quote(username)
        except AttributeError:
            usernameEnc = urllib.quote(username)
        url = baseUrl + usernameEnc
        if (not checkurl(url)):
            print("Check the username ({}) for user {} (the URL is {})".format(username, name, url))
            exit(-1)
        table.append("| " + name + " " * (nameMax - len(name)) + " | [" + username + '](' + url + ")" + " " * (userNameMax + userNameEncMax - len(username) - len(usernameEnc)) + " |")

    return table

def print_JOSM_search(realUsers):
    searchString = ""
    for user in realUsers:
        username = user['username']
        searchString += "user:"
        if ' ' in username:
            searchString += "\\\"" + username + "\\\""
        else:
            searchString += username
        searchString += " or "
    if searchString.endswith(' or '):
        searchString = searchString[:-4]
    searchString = "JOSM_search(\"" + searchString + "\")"
    print("Search string for mapcss files")
    print(searchString)

def updateFiles(files, users):
    if 'USERS' in users:
        realUsers = users['USERS']
    else:
        realUsers = users
    table = buildTable(realUsers)
    print_JOSM_search(realUsers)

    for tfile in files:
        with open(tfile, 'r') as original, open (tfile + '.tmp', 'w') as new:
            tableWritten = False
            writingTable = False
            for line in original:
                if (line.startswith('| Name') and 'OSM Username' in line):
                    tableWritten = True
                    writingTable = True
                    for user in table:
                        new.write(user + '\n')
                elif (writingTable and line.startswith('|')):
                    pass
                elif (writingTable and not line.startswith('|')):
                    writingTable = False
                    new.write(line)
                else:
                    new.write(line)
        os.rename(tfile + '.tmp', tfile)

def main():
    users = getUsers()
    files = findFiles()
    updateFiles(files, users)

if __name__ == "__main__":
    main()
