ARG RUBY_IMAGE_TAG=3.2.1-alpine3.17
FROM docker.io/ruby:${RUBY_IMAGE_TAG} AS base

FROM base AS dependencies

RUN apk -U upgrade \
  && apk add build-base gcompat \
    freetds-dev mariadb-dev curl git

COPY Gemfile Gemfile.lock ./

RUN bundle config set without "development test" \
  && bundle install --jobs=3 --retry=3

FROM base

ENV APP_HOME=/home/app
ENV HISTFILE=${APP_HOME}/.bash_history

RUN apk -U upgrade \
   && apk add gcompat curl \
   && mkdir ${APP_HOME} \
   && chown -R 1001:0 ${APP_HOME} \
   && chmod -R g=u ${APP_HOME}

USER 1001

WORKDIR ${APP_HOME}

COPY --from=dependencies /usr/local/bundle/ /usr/local/bundle/

CMD ["chronos"]
