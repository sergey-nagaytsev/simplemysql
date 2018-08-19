#!/bin/sh

docker run --name simplemysql-postgres -p 5432:5432 -e POSTGRES_DB=test -e POSTGRES_USER=travis -e POSTGRES_PASSWORD= -d postgres:latest
