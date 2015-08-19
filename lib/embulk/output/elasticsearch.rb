Embulk::JavaPlugin.register_output(
  :elasticsearch, "org.embulk.output.elasticsearch.ElasticsearchOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
