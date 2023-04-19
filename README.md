serves as a template to set up an APIi end point that pushes data to kafka.
parsing must be added as per pipeline needs


add config.env using the config.env.template
add topic to kafka before running the app and update ACL to make sure the user has access rights to produce to the topic
add ca certficate for kafka to project root as chain.pem
