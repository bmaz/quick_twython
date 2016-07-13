# quick_twython

## starting out

###Install Twython:
pip install twython

###Set configs:
Rename file "config.py.example" to "config.py".
You can now set collect parameters.

###Get Twitter access tokens:
Head over to https://dev.twitter.com/apps and register an application.
After you register, grab your applications consumer_key, consumer_secret, oauth_token, and oauth_token_secret
Paste them in the config.py file.
If you have several applications registered on twitter it is even better: paste
all your access tokens in the config.py file. Quick_twython is designed to use
all available access tokens to retrieve tweets (see threads.py file)
