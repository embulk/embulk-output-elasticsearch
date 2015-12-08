Embulk::JavaPlugin.register_output(
  :elasticsearch_1x, "org.embulk.output.elasticsearch.ElasticsearchOutputPlugin_1x",
  File.expand_path('../../../../classpath', __FILE__))
