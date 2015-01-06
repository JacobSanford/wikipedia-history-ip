import peewee
from peewee import *
import diff_match_patch as dmp_module
import xml.etree.ElementTree as etree
from netaddr import IPNetwork, IPAddress
import urllib
import urllib2
import re
import os
import subprocess
from datetime import datetime
import time

files_to_process = []

gov_ip_ranges = [
    {'label': 'Province of New Brunswick', 'block': '142.139.0.0/16'},
    {'label': 'Province of New Brunswick', 'block': '162.252.132.0/24'},
    {'label': 'Province of New Brunswick', 'block': '24.137.193.64/26'},
    {'label': 'Province of New Brunswick', 'block': '208.124.210.136/29'},
    {'label': 'Province of New Brunswick', 'block': '208.124.210.144/29'},
    {'label': 'City of Fredericton', 'block': '205.174.161.0/24'},
    {'label': 'City of Fredericton', 'block': '205.174.162.56/29'},
    {'label': 'City of Fredericton', 'block': '198.164.190.0/24'},
    {'label': 'City of Saint John', 'block': '198.164.108.0/24'},
    {'label': 'City of Moncton', 'block': '192.160.167.0/24'},
    {'label': 'CRTC', 'block': '199.246.230.0/24'},
    {'label': 'CRTC', 'block': '199.246.231.0/24'},
    {'label': 'CRTC', 'block': '199.246.232.0/24'},
    {'label': 'CRTC', 'block': '199.246.233.0/24'},
    {'label': 'CRTC', 'block': '199.246.234.0/24'},
    {'label': 'CRTC', 'block': '199.246.235.0/24'},
    {'label': 'CRTC', 'block': '199.246.236.0/24'},
    {'label': 'CRTC', 'block': '199.246.237.0/24'},
    {'label': 'CRTC', 'block': '199.246.238.0/24'},
    {'label': 'CRTC', 'block': '199.246.239.0/24'},
    {'label': 'CRTC', 'block': '199.246.241.0/24'},
    {'label': 'CRTC', 'block': '199.246.242.0/24'},
    {'label': 'CRTC', 'block': '199.246.243.0/24'},
    {'label': 'CRTC', 'block': '199.246.244.0/24'},
    {'label': 'CRTC', 'block': '199.246.245.0/24'},
    {'label': 'CRTC', 'block': '199.246.246.0/24'},
    {'label': 'CRTC', 'block': '199.246.247.0/24'},
    {'label': 'CRTC', 'block': '199.246.248.0/24'},
    {'label': 'CRTC', 'block': '199.246.249.0/24'},
    {'label': 'CRTC', 'block': '199.246.250.0/24'},
    {'label': 'CRTC', 'block': '199.246.251.0/24'},
    {'label': 'CRTC', 'block': '199.246.252.0/24'},
    {'label': 'CRTC', 'block': '199.246.253.0/24'},
    {'label': 'Canada Centre for Inland Waters', 'block': '192.75.68.0/24'},
    {'label': 'Canada Centre for Inland Waters', 'block': '198.73.135.0/24'},
    {'label': 'Canada Centre for Inland Waters', 'block': '198.73.136.0/24'},
    {'label': 'Canada Centre for Inland Waters', 'block': '205.189.5.0/24'},
    {'label': 'Canada Centre for Inland Waters', 'block': '205.189.6.0/24'},
    {'label': 'Canada Centre for Inland Waters', 'block': '205.189.7.0/24'},
    {'label': 'Canadian Department of National Defence', 'block': '131.132.0.0/16'},
    {'label': 'Canadian Department of National Defence', 'block': '131.133.0.0/16'},
    {'label': 'Canadian Department of National Defence', 'block': '131.134.0.0/16'},
    {'label': 'Canadian Department of National Defence', 'block': '131.135.0.0/16'},
    {'label': 'Canadian Department of National Defence', 'block': '131.136.0.0/16'},
    {'label': 'Canadian Department of National Defence', 'block': '131.137.0.0/16'},
    {'label': 'Canadian Department of National Defence', 'block': '131.138.0.0/16'},
    {'label': 'Canadian Department of National Defence', 'block': '131.139.0.0/16'},
    {'label': 'Canadian Department of National Defence', 'block': '131.140.0.0/16'},
    {'label': 'Canadian Department of National Defence', 'block': '131.141.0.0/16'},
    {'label': 'Canadian House of Commons', 'block': '192.197.82.0/24'},
    {'label': 'Canadian Hydropgraphic Service', 'block': '204.187.48.0/24'},
    {'label': 'Correction Services Canada', 'block': '142.191.8.0/21'},
    {'label': 'Correction Services Canada', 'block': '142.236.0.0/17'},
    {'label': 'Department of Fisheries and Oceans', 'block': '192.197.244.0/24'},
    {'label': 'Department of Fisheries and Oceans', 'block': '192.139.141.0/24'},
    {'label': 'Department of Fisheries and Oceans', 'block': '192.197.243.0/24'},
    {'label': 'Department of Fisheries and Oceans', 'block': '198.103.161.0/24'},
    {'label': 'Department of Justice Canada', 'block': '199.212.200.0/24'},
    {'label': 'Department of Justice Canada', 'block': '199.212.215.0/24'},
    {'label': 'Department of Justice Canada', 'block': '199.212.216.0/24'},
    {'label': 'Employment and Immigration Canada', 'block': '167.227.0.0/16'},
    {'label': 'Federal Court of Canada', 'block': '198.103.145.0/24'},
    {'label': 'Environment Canada', 'block': '199.212.16.0/24'},
    {'label': 'Environment Canada', 'block': '199.212.17.0/24'},
    {'label': 'Environment Canada', 'block': '199.212.18.0/24'},
    {'label': 'Environment Canada', 'block': '199.212.19.0/24'},
    {'label': 'Environment Canada', 'block': '199.212.20.0/24'},
    {'label': 'Environment Canada', 'block': '199.212.21.0/24'},
    {'label': 'Environment Canada', 'block': '205.189.8.0/24'},
    {'label': 'Environment Canada', 'block': '205.189.9.0/24'},
    {'label': 'Environment Canada', 'block': '205.189.10.0/24'},
    {'label': 'Environment Canada', 'block': '205.211.132.0/24'},
    {'label': 'Environment Canada', 'block': '205.211.133.0/24'},
    {'label': 'Environment Canada', 'block': '205.211.134.0/24'},
    {'label': 'Finance Canada', 'block': '198.103.32.0/20'},
    {'label': 'Finance Canada', 'block': '198.103.48.0/24'},
    {'label': 'Finance Canada', 'block': '198.103.49.0/24'},
    {'label': 'Finance Canada', 'block': '198.103.50.0/24'},
    {'label': 'Finance Canada', 'block': '198.103.51.0/24'},
    {'label': 'Finance Canada', 'block': '198.103.52.0/24'},
    {'label': 'Finance Canada', 'block': '198.103.53.0/24'},
    {'label': 'Finance Canada', 'block': '198.103.54.0/24'},
    {'label': 'Finance Canada', 'block': '198.103.55.0/24'},
    {'label': 'Finance Canada', 'block': '198.103.56.0/24'},
    {'label': 'Finance Canada', 'block': '198.103.57.0/24'},
    {'label': 'Finance Canada', 'block': '198.103.58.0/24'},
    {'label': 'Finance Canada', 'block': '198.103.59.0/24'},
    {'label': 'Finance Canada', 'block': '198.103.60.0/24'},
    {'label': 'Finance Canada', 'block': '198.103.61.0/24'},
    {'label': 'Finance Canada', 'block': '198.103.62.0/24'},
    {'label': 'Finance Canada', 'block': '198.103.63.0/24'},
    {'label': 'Foreign Affairs Canada', 'block': '216.174.155.0/28'},
    {'label': 'Government Of Canada', 'block': '192.139.201.0/24'},
    {'label': 'Government Of Canada', 'block': '192.139.202.0/24'},
    {'label': 'Government Of Canada', 'block': '192.139.203.0/24'},
    {'label': 'Government Of Canada', 'block': '192.139.204.0/24'},
    {'label': 'Government Of Canada', 'block': '192.197.77.0/24'},
    {'label': 'Government Of Canada', 'block': '192.197.78.0/24'},
    {'label': 'Government Of Canada', 'block': '192.197.80.0/24'},
    {'label': 'Government Of Canada', 'block': '192.197.84.0/24'},
    {'label': 'Government Of Canada', 'block': '192.197.86.0/24'},
    {'label': 'Health Canada', 'block': '204.187.49.0/24'},
    {'label': 'Industry Canada', 'block': '192.197.183.0/24'},
    {'label': 'Industry Canada', 'block': '161.187.0.0/16'},
    {'label': 'Industry Canada', 'block': '142.53.0.0/16'},
    {'label': 'Industry Canada', 'block': '192.197.185.0/24'},
    {'label': 'Industry Canada', 'block': '192.197.178.0/24'},
    {'label': 'Industry Canada', 'block': '192.197.179.0/24'},
    {'label': 'Industry Canada', 'block': '192.197.180.0/24'},
    {'label': 'Industry Canada', 'block': '192.197.181.0/24'},
    {'label': 'Industry Canada', 'block': '192.197.182.0/24'},
    {'label': 'Industry Canada', 'block': '192.197.184.0/24'},
    {'label': 'Library and Archives Canada', 'block': '142.78.0.0/16'},
    {'label': 'Service Canada', 'block': '216.174.155.0/28'},
    {'label': 'Shared Services Canada', 'block': '167.55.240.0/24'},
    {'label': 'Statistics Canada', 'block': '142.206.0.0/16'},
    {'label': 'Natural Resources Canada', 'block': '132.156.4.0/23'},
    {'label': 'Natural Resources Canada', 'block': '132.156.10.0/23'},
    {'label': 'Natural Resources Canada', 'block': '132.156.12.0/23'},
    {'label': 'Natural Resources Canada', 'block': '132.156.16.0/23'},
    {'label': 'Natural Resources Canada', 'block': '132.156.20.0/22'},
    {'label': 'Natural Resources Canada', 'block': '132.156.46.0/23'},
    {'label': 'Natural Resources Canada', 'block': '132.156.120.0/23'},
    {'label': 'Natural Resources Canada', 'block': '192.67.45.0/24'},
    {'label': 'Natural Resources Canada', 'block': '192.75.99.0/24'},
    {'label': 'Natural Resources Canada', 'block': '192.139.6.0/24'},
    {'label': 'Natural Resources Canada', 'block': '192.139.7.0/24'},
    {'label': 'Natural Resources Canada', 'block': '192.139.194.0/24'},
    {'label': 'Natural Resources Canada', 'block': '132.156.0.0/16'},
    {'label': 'Natural Resources Canada', 'block': '192.197.114.0/24'},
    {'label': 'Natural Resources Canada', 'block': '192.197.115.0/24'},
    {'label': 'Transport Canada', 'block': '142.211.0.0/16'},
    {'label': 'Transport Canada', 'block': '142.209.0.0/16'},
    {'label': 'Transport Canada', 'block': '142.210.0.0/16'},
    {'label': 'Transport Canada', 'block': '198.103.96.0/24'},
]

bytes_per_page = 23000
update_progress_every= 10000

# Init Database
db = MySQLDatabase('wikiparser', user='wikiuser',passwd='p444rs333')

class AnonWikiEdit(peewee.Model):
    wikifile = peewee.CharField(255)
    pageid = peewee.IntegerField()
    revisionid = peewee.IntegerField()
    datetime = peewee.DateTimeField()
    ipblocklabel = peewee.CharField(255)
    ipaddress = peewee.CharField(255)
    title = peewee.CharField(255)
    pagecontents = peewee.BlobField()
    diff = peewee.BlobField()

    class Meta:
        database = db

if not AnonWikiEdit.table_exists():
    AnonWikiEdit.create_table()

media_wiki_ns = "{http://www.mediawiki.org/xml/export-0.8/}"
media_wiki_ns_page = media_wiki_ns + 'page'
media_wiki_ns_title = media_wiki_ns + 'title'
media_wiki_ns_revision = media_wiki_ns + 'revision'
media_wiki_ns_contributor = media_wiki_ns + 'contributor'
media_wiki_ns_ip = media_wiki_ns + 'ip'
media_wiki_ns_id = media_wiki_ns + 'id'
media_wiki_ns_parentid = media_wiki_ns + 'parentid'
media_wiki_ns_timestamp = media_wiki_ns + 'timestamp'
media_wiki_ns_text = media_wiki_ns + 'text'
media_wiki_ns_username = media_wiki_ns + 'username'

wikipedia_dump_page_url = "http://dumps.wikimedia.org/enwiki/latest/"
wikipedia_dump_page = urllib2.urlopen(wikipedia_dump_page_url)
wikipedia_dump_page = wikipedia_dump_page.read()
full_wikipedia_dump_links = re.findall(r"<a.*?\s*href=\"(enwiki-latest-pages-meta-history.*?\.7z)\".*?>(.*?)</a>", wikipedia_dump_page)
for link in full_wikipedia_dump_links:
    files_to_process.append(link[0])
    # pass
    # print('href: %s, HTML text: %s' % (link[0], link[1]))

found_count = 0
file_count = 0
last_timestamp = 0.0

num_files = len(files_to_process)

for cur_file_to_process in files_to_process:
    file_count += 1
    print "Downloading " + cur_file_to_process
    file_to_download = wikipedia_dump_page_url + cur_file_to_process
    testfile=urllib.URLopener()
    testfile.retrieve(file_to_download,"cur_dump.7z")

    print "Extracting " + cur_file_to_process
    command = ["7z", "-y", "x", "cur_dump.7z"]
    subprocess.call(command)

    wiki_file_to_parse = 'cur_dump'

    print "Parsing " + cur_file_to_process

    XML_FILE = open(wiki_file_to_parse, 'r')
    context = etree.iterparse(XML_FILE, events=("start", "end"))
    context = iter(context)
    event, root = context.next()

    # Get Count
    parse_count = 0
    for event, elem in context:
        if event == "end" and elem.tag == media_wiki_ns_page:
            article_ip_edits = elem.find(media_wiki_ns_revision + '/' + media_wiki_ns_contributor + '/' + media_wiki_ns_ip)
            if not article_ip_edits is None:
                # Find Title
                for cur_ip_block in gov_ip_ranges:
                    if IPAddress(article_ip_edits.text) in IPNetwork(cur_ip_block['block']):
                        cur_article_title = elem.find(media_wiki_ns_title).text
                        cur_page_id = elem.find(media_wiki_ns_id).text

                        # Load Revisions Into Dict
                        cur_revision_dict = {}
                        cur_revision_dict['anonymous'] = []
                        for cur_revision in elem.findall(media_wiki_ns_revision):
                            revision_id = cur_revision.find(media_wiki_ns_id).text

                            cur_revision_dict[revision_id] = {}

                            if not cur_revision.find(media_wiki_ns_parentid) is None:
                                cur_revision_dict[revision_id]['parentid'] = cur_revision.find(media_wiki_ns_parentid).text
                            else:
                                cur_revision_dict[revision_id]['parentid'] = '0'
                            cur_revision_dict[revision_id]['text'] = cur_revision.find(media_wiki_ns_text).text
                            cur_revision_dict[revision_id]['timestamp'] = cur_revision.find(media_wiki_ns_timestamp).text
                            cur_contributor = cur_revision.find(media_wiki_ns_contributor)
                            if not cur_contributor.find(media_wiki_ns_username) is None:
                                cur_revision_dict[revision_id]['username'] = cur_contributor.find(media_wiki_ns_username).text
                                cur_revision_dict[revision_id]['userid'] = cur_contributor.find(media_wiki_ns_id).text
                            else:
                                cur_revision_dict[revision_id]['ip'] = cur_contributor.find(media_wiki_ns_ip).text
                                cur_revision_dict['anonymous'].append(revision_id)
                        # Iterate over anonmyous revisions found
                        for cur_revision_id in cur_revision_dict['anonymous']:
                            cur_revision_text = cur_revision_dict[revision_id]['text']
                            last_revision_text = None if cur_revision_dict[cur_revision_id]['parentid'] is 0 else cur_revision_dict[cur_revision_dict[cur_revision_id]['parentid']]['text']

                            try:
                                AnonWikiEdit.get(AnonWikiEdit.revisionid == cur_revision_id)
                            except DoesNotExist:
                                dmp = dmp_module.diff_match_patch()
                                dmp.Diff_Timeout = 0.5  # Don't spend more than 0.1 seconds on a diff.
                                cur_revision_diff = dmp.diff_prettyHtml(dmp.diff_main(last_revision_text, cur_revision_text))
                                wiki_edit_record = AnonWikiEdit(wikifile=wiki_file_to_parse, pageid=cur_page_id, revisionid=cur_revision_id, datetime=datetime.strptime(cur_revision_dict[revision_id]['timestamp'], '%Y-%m-%dT%H:%M:%SZ'), ipblocklabel=cur_ip_block['label'], ipaddress=cur_revision_dict[cur_revision_id]['ip'], title=cur_article_title, pagecontents=cur_revision_text, diff=cur_revision_diff)
                                wiki_edit_record.save()
                                print cur_ip_block['label'], cur_revision_dict[cur_revision_id]['ip'], cur_revision_dict[cur_revision_id]['parentid'], '->', cur_revision_id, ':', cur_revision_dict[revision_id]['timestamp']
                                found_count += 1
            parse_count += 1
            if (parse_count % update_progress_every) == 0:
                cur_timestamp = time.time()
                if not last_timestamp == 0:
                    cycle_time = cur_timestamp - last_timestamp
                else:
                    cycle_time = 0.0
                print str(file_count) + '/' + str(num_files), cur_file_to_process, parse_count, str(found_count) + ' found', cycle_time
                last_timestamp = cur_timestamp

        elem.clear()
    XML_FILE.close()
