#!/bin/bash
cd ../..
source venv/bin/activate
python main.py mbank-parse 'Analityka finansowa'
cd runners/bash