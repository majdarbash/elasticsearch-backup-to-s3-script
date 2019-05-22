########################################################################################
# ATTENTION !!!                                                                       ##
# The current script is designed to PERMANENTLY DELETE indices once                   ##
# their snapshots are generated.                                                      ##
# Wrong configuration may lead to permanent deletion of indices                       ##
#                                                                                     ##
# TEST BEFORE USE !                                                                   ##
########################################################################################

import os, datetime, json, time

# TODO: covert these variables to arguments to be passed from the cli
# URL to ElasticSearch - make sure the current script has access to ElasticSearch
# if you are using AWS ElasticSearch, access can be granted using Policy
endpoint = "https://endpoint"
# Number of days of index you would like to retain
retention_duration_days = 10
# Whether to dry run or to apply changes
dry_run = False
# Indexes excluded from the rotation process
# Under these circumstances, the excluded indexes will not be archived, however all other
# indexes will be archived based on the days retention argument specified above
excluded_indices = ['.kibana']

def clear_screen():
    print(chr(27) + "[2J")

clear_screen()

current_day = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
retention_till_day = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=retention_duration_days)

print "Today is {0}".format(current_day)
print "Indexes since {0} will be retained".format(retention_till_day)


print "Retrieving indices:"
command = "curl -s -XGET " + endpoint + "/_cat/indices/logstash* | awk '{ print $3 }'"
process = os.popen(command)
result = process.read()
process.close()

found_incides = result.split("\n")

print
print "Found total number of indices: {0}".format(len(found_incides))
for index in found_incides:
    time.sleep(5)
    index = index.strip()
    if index == "":
        continue
    print
    print "Index found {0}".format(index)

    if index in excluded_indices:
        print "Index {0} found in exclusion list. Skipping it.".format(index)
        continue

    date_from_index_name = index.replace("logstash-", "")
    try:
        index_date = datetime.datetime.strptime(date_from_index_name, "%Y.%m.%d")
    except:
        continue

    if index_date < retention_till_day:
        # these indices have to be backed-up and deleted if already not backed up before
        try:
            print "Backing up index: " + index_date.__format__("%Y/%m/%d")
        except:
            print "There was a problem backing up this index: {0}, deleting it.".format(index_date)
            if not dry_run:
                command = "curl -s -XDELETE " + endpoint + "//" + index
                print command
                process = os.popen(command)
                result = process.read()
                process.close()
            else:
                result = "DRYRUN"

            print "Deletion command executed with response: {0}".format(result)
            continue

        # check if the snapshot already exists
        # if done, delete the index - otherwise, skip until snapshot is successfully created
        snapshot_name = "backup_" + date_from_index_name
        print "Searching for snapshot {0}".format(snapshot_name)
        command = "curl -s -XGET " + endpoint + "/_snapshot/backup/" + snapshot_name + "/_status"
        print command
        process = os.popen(command)
        result = process.read()
        process.close()
        result = json.loads(result)

        if 'snapshots' in result and len(result['snapshots']) > 0:
            if result['snapshots'][0]['state'] == 'SUCCESS':
                print "Snapshot is available for index {0} - can delete the index.".format(index)
                print "Deleting index: {0}".format(index)

                if not dry_run:
                    command = "curl -s -XDELETE " + endpoint + "//" + index
                    print command
                    process = os.popen(command)
                    result = process.read()
                    process.close()
                else:
                    result = "DRYRUN"

                print "Deletion command executed with response: {0}".format(result)

                continue
            else:
                print "Snapshot was requested for index {0} and is currently in {1} state. Skipping it.".format(index, result['snapshots'][0]['state'])
                continue

        elif 'error' in result and result['error']['type'] == 'snapshot_missing_exception':
            # if snapshot does not exist, request for it
            if not dry_run:
                command = "curl -s -XPUT " + endpoint + "/_snapshot/backup/backup_" + date_from_index_name + "?wait_for_completion=true -d'{\"indices\": \"" + index + "\"}' "
                print command
                process = os.popen(command)
                result = process.read()
                process.close()
            else:
                result = "DRYRUN"

            print "Snapshot creation command returned result {0}".format(result)

        else:
            print "Unexpected response when searching for snapshot {0}. Response: {1}".format(snapshot_name, result)

    else:
        print "Index {0} will be retained. Skipping it".format(index)
