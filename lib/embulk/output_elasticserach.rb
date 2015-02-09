Embulk::JavaPlugin.register_input(
  :elasticserach, "org.embulk.plugin.elasticsearch.ElasticsearchOutputPlugin",
  File.expand_path('../../../classpath', __FILE__))
