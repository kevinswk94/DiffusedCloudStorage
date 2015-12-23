echo on
set installpath=c:\svdfs
set masterpath=%installpath%\master
REM delete old data and start afresh
del %masterpath%\namespaceTrans.log /Q
del %masterpath%\svds.img /Q
del %masterpath%\svdsTrans.log /Q
cd %masterpath%
start cmd /c start_svdsm.bat