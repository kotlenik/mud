#!/bin/bash

./rama deploy \
--action launch \
--jar $1 \
--module 'mud.module/BankDemo' \
--tasks 4 \
--threads 2 \
--workers 1
