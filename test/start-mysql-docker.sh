#!/bin/sh

docker run --name simplemysql-mysql -p 3306:3306 -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -e MYSQL_DATABASE=test -e MYSQL_USER=travis -e MYSQL_PASSWORD= -d mysql:5
