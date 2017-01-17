# quick_twython

## starting out

###Install Elasticsearch:
https://www.elastic.co/guide/en/elasticsearch/reference/current/_installation.html  
Allow dynamic scripting:  
in elasticsearch-x.y.z/config/elasticsearch.yml, add:  
script.inline: on  
script.indexed: on  

###Install Twython, Elasticsearch modules:
pip install twython  
pip install elasticsearch  

###Set configs:
Rename file "config.py.example" to "config.py".
You can now set collect parameters.

###Get Twitter access tokens:
Head over to https://dev.twitter.com/apps and register an application.
After you register, grab your applications consumer_key, consumer_secret, oauth_token, and oauth_token_secret.  
Paste them in the config.py file.  
If you have several applications registered on twitter it is even better: paste
all your access tokens in the config.py file. Quick_twython is designed to use
all available access tokens to retrieve tweets.  


## Word2Vec Model
You can have access to my test word2vec model trained on 2 million french tweets collected between October 3th and October 7th 2016.
Tweets were treated to remove urls, "@" mentions, punctuation and uppercases. Gensim phrase detection tool was used to detect common bigrams such as "françois_hollande".