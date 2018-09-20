FROM ubuntu:18.04

RUN apt-get update -y

# get add-apt-repository
RUN apt-get install -y nginx software-properties-common
RUN bash -c 'DEBIAN_FRONTEND=noninteractive apt-get install -y tzdata'

RUN add-apt-repository -y ppa:certbot/certbot
RUN apt-get update -y
RUN apt-get install -y python-certbot-nginx

RUN rm -f /etc/nginx/sites-enabled/default
ADD test.hail.is.conf /etc/nginx/conf.d/test.hail.is.conf
ADD index.html /var/www/html/index.html

CMD ["nginx", "-g", "daemon off;"]
