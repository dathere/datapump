@echo off
CALL ..\Scripts\activate
python datapump.py --config datapump.ini
deactivate
