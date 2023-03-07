ARG RUBY_IMAGE_TAG=3.2.1-alpine3.17
FROM docker.io/ruby:${RUBY_IMAGE_TAG} AS base

ENV CHRONOS_HOME=/opt/chronos
ENV BUNDLE_PATH=${CHRONOS_HOME}/.bundle
ENV APP_HOME=/home/app

RUN apk -U upgrade \
    && mkdir ${CHRONOS_HOME} \
    && mkdir ${APP_HOME}

FROM base AS dependencies

RUN apk add build-base curl git \
      freetds-dev mariadb-dev

WORKDIR ${CHRONOS_HOME}

COPY Gemfile ./

RUN bundle config set without "development test" \
    && bundle install --jobs=3 --retry=3 \
    && bundle binstubs chronos

FROM base

ENV HISTFILE=${APP_HOME}/.bash_history
ENV PATH=${CHRONOS_HOME}/bin:$PATH

COPY --from=dependencies ${CHRONOS_HOME} ${CHRONOS_HOME}

RUN apk add gcompat curl freetds-dev mariadb-dev \
    && chown -R 1001:0 ${CHRONOS_HOME} \
    && chmod -R g=u ${CHRONOS_HOME} \
    && chown -R 1001:0 ${APP_HOME} \
    && chmod -R g=u ${APP_HOME}
    
USER 1001

WORKDIR ${APP_HOME}

CMD ["chronos"]
