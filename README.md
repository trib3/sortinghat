# build
<code>docker build --rm -t sorting-hat:latest .</code>

# run locally
<code> docker-compose --file docker-compose-LocalExecutor.yml up </code>

# register container with AWS
<code>
docker tag sorting-hat:latest 245739941934.dkr.ecr.us-east-1.amazonaws.com/sorting-hat:latest
docker push 245739941934.dkr.ecr.us-east-1.amazonaws.com/sorting-hat:latest
</code>

#run on AWS
<code> ecs-cli compose --file docker-compose-LocalExecutor.yml up </code>