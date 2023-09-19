wsgi_app = "deployment.wsgi.wsgi:application"
bind = "0.0.0.0:8000"
workers = 1
user = "appuser"
group = "appuser"
timeout = 600  # Increase the timeout to 600 seconds (10 minutes)
