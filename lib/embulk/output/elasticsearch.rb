module Embulk
  module Output
    module Elasticsearch
      V_1 = File.expand_path('../../../../classpath_v1', __FILE__)
      V_2 = File.expand_path('../../../../classpath_v2', __FILE__)
    end
  end
end

Embulk::JavaPlugin.register_output(
  :elasticsearch, "org.embulk.output.elasticsearch.ElasticsearchOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
