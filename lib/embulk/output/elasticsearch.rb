Embulk::JavaPlugin.register_output(
  :elasticsearch, "org.embulk.output.ElasticsearchOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
