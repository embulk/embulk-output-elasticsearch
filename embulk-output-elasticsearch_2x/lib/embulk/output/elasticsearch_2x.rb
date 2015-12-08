Embulk::JavaPlugin.register_output(
  :elasticsearch_2x, "org.embulk.output.elasticsearch.ElasticsearchOutputPlugin_2x",
  File.expand_path('../../../../classpath', __FILE__))
