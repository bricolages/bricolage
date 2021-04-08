FROM ruby:3.0-slim

RUN apt-get update -qq && apt-get install -y build-essential libpq-dev libpq5 postgresql-client

WORKDIR /app

COPY ./Gemfile .
COPY ./Gemfile.lock .
RUN gem install bundler:2.2.15 && \
    bundle config set --local deployment true && \
    bundle config set --local without 'development test' && \
    bundle install -j4

COPY . .

CMD ["bundle", "exec", "ridgepole", "-f", "Schemafile", "-c", "database.yml", "--merge", "--dry-run"]
