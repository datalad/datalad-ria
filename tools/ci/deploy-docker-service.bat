set DOCKERRIASERVICE=%1
curl -fsSL -o %DOCKERRIASERVICE%.dockerimg "https://ci.appveyor.com/api/projects/mih/datalad-ria/artifacts/cache/%DOCKERRIASERVICE%.dockerimg"
docker load < %DOCKERRIASERVICE%.dockerimg
docker run --rm -dit --name %* %DOCKERRIASERVICE%
:: cleanup, we do not need the image anymore, and prevent reupload
:: as an artifact
del %DOCKERRIASERVICE%.dockerimg
