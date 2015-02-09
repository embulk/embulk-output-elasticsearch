Embulk::JavaPlugin.register_output(
  :elasticsearch, "org.embulk.plugin.elasticsearch.ElasticsearchOutputPlugin",
  File.expand_path('../../../classpath', __FILE__))
