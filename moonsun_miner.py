"""
MoonSun_Miner - A tool for extracting data from LUNA's Solr backend.
Author: Mike Bennett (mike.bennett@ed.ac.uk)
Requirements: json, requests
"""

import sys,requests,json

def luna_login(session):
    """Authenticate the requests Session object to Luna by posting user/pass to the login form"""
    form_url = 'http://images.is.ed.ac.uk/las/login.htm'
    form_payload = {'username': 'lunauser',
                    'password': 'yourpasswordhere'}

    login_attempt = session.post(form_url, data=form_payload)

    if login_attempt.status_code == 200:
        return True
    else:
        print("Login Failed: %s - %s"%(login_attempt.status_code, login_attempt.reason))
        return False


def solr_query(session, query, limit = 0, fields=[], sort=[]):
    """Construct a query, send it to Solr, paginate through results and return"""
    solr_url = 'http://images.is.ed.ac.uk/las/solr/select'
    base_qstring = '?indent=on&version=2.2&qt=standard&wt=json'
    start = 0
    results = []

    # Build the querystring
    qstring = base_qstring + "&q=" + query
    qstring = qstring + "&fl=" + ",".join(fields)
    qstring = qstring + "&sort=" + ",".join(sort)
    if limit > 0 and limit < 100:
        qstring = qstring + "&rows=%s"%limit
    else:
        qstring = qstring + "&rows=100"

    # Start the loop for pulling results
    is_last = False
    while not is_last:
        url = solr_url + qstring + "&start=%s"%start
        response = session.get(url)

        if response.status_code == 200:
            # Let's try and parse the results and see what happens!
            try:
                results_page = json.loads(response.text)
                results.extend(results_page['response']['docs'])
            except Exception as e:
                print("Error parsing JSON from response")
                print(e)
                sys.exit(1)

            # Results obtained, increment start and exit loop if results finished
            total_results = results_page['response']['numFound']
            start = start + 100 # If you change this, make sure you change the 'rows' value in the base_qstring
            if start > total_results or (0 < limit < start): is_last = True
        else:
            # Uh-oh!
            print("Error obtaining results from Solr: %s - %s"%(response.status_code,response.reason))
            sys.exit(1)

    # So in theory, we now have a nice list of results, let's check!
    if len(results) > 0:
        print("%s results found"%len(results))
        for result in results:
            if type(result) != dict:
                print("Non-dictionary result found. If this happens, something has gone very wrong!")
                print(result)
    else:
        print("No results found")

    return results

if __name__ == "__main__":
    print("Moonsun Miner - Data extraction from a LUNA/Solr instance")
    print("Direct script launch detected, running tests...")
    print("Initialising session")
    try:
        s = requests.Session()
    except Exception as e:
        print("Error: %s"%e)
        sys.exit(1)

    print("Session initialised, logging into LUNA")
    try:
        if luna_login(s):
            print("Successful login")
        else:
            sys.exit(1) # Error reason should have already been printed by the login procedure
    except Exception as e:
        print("Error: %s"%e)
        sys.exit(1)
    print("Querying for 10 records")
    try:
        results = solr_query(s,"*:*",10)
        if len(results) == 10:
            print("Successful query")
            print("All tests passed")
            sys.exit(0)
        else:
            print("Error obtaining results")
            print("Results dump:")
            print(results)
            sys.exit(1)
    except Exception as e:
        print("Error: %s" % e)
        sys.exit(1)

