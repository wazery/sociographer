require 'active_support/concern'

module Sociographer
  module Actionable
    extend ActiveSupport::Concern
    included do

      after_create :create_actionable
      before_destroy :ensure_deletion_fixes

      def get_node_id(query)
        if query
          response = $neo.execute_query(query)
          node_id = response['data'].flatten.first['metadata']['id']
        end
      end

      # Get only the ID of the corresponding node to the entity
      def entity_node_id
        node = nil          
        begin
          node = Neography::Node.find('actionables_nodes_index', 'class0id', "#{self.class.name}0#{self.id}")
        rescue
        end
        if node 
          node_id = node.neo_id.to_i
        else
          query = "MATCH (n {object_id: #{self.id.to_s}, object_type: \'#{self.class.name}\' }) RETURN n LIMIT 1"
          get_node_id(query)
        end
      end

      def get_node(node_id)
        node_id ? Neography::Node.load(node_id, $neo) : nil
      end

      # Get the corresponding node to the entity
      def entity_node(node_id=nil)
        node = nil
        if node_id
          node = get_node(node_id)
        else
          begin
            node = Neography::Node.find('actionables_nodes_index', 'class#id', "#{self.class.name}##{self.id}")
          rescue
          end
          unless node
            node = get_node(self.entity_node_id)
          end
        end
        return node
      end

      # Callback after creation of Actionable (e.g. Post)
      # to create the corresponding node in Neo4j DB
      def create_actionable
        actionable_node = Neography::Node.create('object_id' => self.id, 'object_type' => self.class.to_s)
        actionable_node.add_to_index('actionables_nodes_index', 'class0id', "#{self.class.to_s}0#{self.id}")
      end

      def ensure_deletion_fixes
        self_node = self.entity_node
        $neo.remove_node_from_index('actionables_nodes_index', self_node)
        self_node.del
      end
    end
  end
end
