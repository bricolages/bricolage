FROM ruby:2.6.5-stretch

RUN apt-get update -qq && apt-get install -y build-essential libpq-dev libpq5 postgresql-client

COPY ./Gemfile /tmp/Gemfile
COPY ./Gemfile.lock /tmp/Gemfile.lock
RUN cd /tmp && bundle install -j4 --deployment --without 'development test'

WORKDIR /app
COPY . /app
RUN cp -a /tmp/vendor /app/

CMD ["bundle", "exec", "ridgepole", "-f", "Schemafile", "-c", "database.yml", "--merge", "--dry-run"]d

