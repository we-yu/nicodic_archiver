#!/bin/bash

if [ "$1" = "" ] || [ "$2" = "" ]; then
  echo "Usage:"
  echo "  ./inspect.sh <article_id> <article_type> [last N]"
  exit 1
fi

ARTICLE_ID=$1
ARTICLE_TYPE=$2

if [ "$3" = "last" ] && [ "$4" != "" ]; then
  docker compose run --rm scraper inspect $ARTICLE_ID $ARTICLE_TYPE --last $4
else
  docker compose run --rm scraper inspect $ARTICLE_ID $ARTICLE_TYPE
fi

