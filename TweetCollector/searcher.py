# -*- coding: utf-8 -*-
"""
Created on Thu May  7 12:50:00 2020

@author: Teo Victor
"""

import json
import yaml
from searchtweets import load_credentials, gen_rule_payload, ResultStream

class Searcher:
  def __init__(self, key, secret):
    self.settings = dict(
       search_tweets_api=dict(
          account_type='premium',
          consumer_key=key,
          consumer_secret=secret
       ) 
    )
  
  def arquive_search(self, query, start, end, dev_env, max_size=2500, max_call=100):
    self.settings['search_tweets_api']['endpoint'] =\
       f"https://api.twitter.com/1.1/tweets/search/fullarchive/{dev_env}.json"
       
    credentials = load_credentials("archive_keys.yaml",
                                       yaml_key="search_tweets_api",
                                       env_overwrite=False)
       
    with open('archive_keys.yaml', 'w') as config_file:
       yaml.dump(self.settings, config_file, default_flow_style=False)
       
    q_rule = gen_rule_payload(query,
                        results_per_call=max_call,
                        from_date=start,
                        to_date=end)
    
    rs = ResultStream(rule_payload=q_rule,
                          max_results=max_size,
                          **credentials)
    
    with open('tweet_data_archive.csv', 'a', encoding='utf-8') as file:
       n = 0
       for tweet in rs.stream():
          n += 1
          if n % (max_size/10) == 0:
              print('{0}: {1}'.format(str(n), tweet['created_at']))
          json.dump(tweet, file)
          file.write('\n')
