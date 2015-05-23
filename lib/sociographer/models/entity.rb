require 'active_support/concern'

module Sociographer
  module Entity
    extend ActiveSupport::Concern
    included do

      after_create :setup_nodes
      before_destroy :ensure_deletion_fixes

      ### SETUP STARTED ####

      # Callback after creation of Entity (e.g. User)
			# to create the corresponding node in Neo4j DB
      def setup_nodes
        entity_node = self.create_entity_node
        privacy_node = self.create_privacy_node
        activities_node = self.create_activities_node
        activities_list_node = self.create_activities_list_node
        Neography::Relationship.create(:privacy, entity_node, privacy_node)
        Neography::Relationship.create(:activities, entity_node, activities_node)
        Neography::Relationship.create(:activities_list, activities_node, activities_list_node)
        entity_node[:privacy_node_id] = privacy_node.try(:neo_id).try(:to_i)
        entity_node[:activities_node_id] = activities_node.try(:neo_id).try(:to_i)
        activities_list_node_id = activities_list_node.try(:neo_id).try(:to_i)
        entity_node[:activities_list_node_id] = activities_list_node_id
        activities_node[:activities_list_node_id] = activities_list_node_id
        # return entity_node
      end

      def create_entity_node
        self_node = Neography::Node.create( "object_type" => self.class.to_s, "object_id" => self.id, "top_follower_weight" => 0)
        self_node.add_to_index("entities_nodes_index", "class0id", "#{self.class.name}0#{self.id}")
        return self_node
      end

      def create_privacy_node
        privacy_node = Neography::Node.create("object_type" => "privacy", "refrence_id" => self.id, "refrence_type" => self.class.to_s)
        privacy_node[:banned_list] = YAML.dump []
        privacy_node[:favorite_list] = YAML.dump []
        return privacy_node
      end

      def create_activities_node
        activities_node = Neography::Node.create("object_type" => "activities", "refrence_id" => self.id, "refrence_type" => self.class.to_s)
        activities_node[:past_activities_count] = 1
        activities_node[:current_activities_count] = 0
        activities_node[:activities_sets] = YAML.dump []
        activities_node[:activities_counts] = YAML.dump({})
        activities_node[:entities_weights] = YAML.dump({})
        return activities_node
      end

      def create_activities_list_node
        activities_list_node = Neography::Node.create("object_type" => "activities_list", "refrence_id" => self.id, "refrence_type" => self.class.to_s)
        activities_list_node[:activities] = YAML.dump []
        return activities_list_node
      end


      ### SETUP FINISHED ####


      ### NODES IDs FETCHING ####

      def get_node_id(query)
        if query
          response = $neo.execute_query(query)
          node_id = response["data"].flatten.first["metadata"]["id"]
        end
      end

      # Get only the id of the corresponding node to the entity
      def entity_node_id
        node_id = nil          
        begin
          node = Neography::Node.find("entities_nodes_index", "class0id", "#{self.class.name}0#{self.id}")
        rescue
        end
        if node 
          node_id = node.neo_id.to_i
        else
          query = "MATCH (n {object_id: #{self.id.to_s}, object_type: \'#{self.class.name}\' }) RETURN n LIMIT 1"
          get_node_id(query)
        end
      end

      def privacy_node_id
        query = "MATCH (n {refrence_id: #{self.id.to_s}, object_type: \'privacy\', refrence_type: \'#{self.class.name}\' }) RETURN n LIMIT 1"
        get_node_id(query)
      end

      def activities_node_id
        query = "MATCH (n {refrence_id: #{self.id.to_s}, object_type: \'activities\', refrence_type: \'#{self.class.name}\' }) RETURN n LIMIT 1"
        get_node_id(query)
      end

      def activities_list_node_id
        query = "MATCH (n {refrence_id: #{self.id.to_s}, object_type: \'activities_list\', refrence_type: \'#{self.class.name}\' }) RETURN n LIMIT 1"
        get_node_id(query)
      end

      ### NODES IDs FETCHING FINISHED ###

      ### NODES FETCHING ####

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
            node = Neography::Node.find("entities_nodes_index", "class0id", "#{self.class.name}0#{self.id}")
          rescue
          end
          unless node
            node = get_node(self.entity_node_id)
          end
        end
        return node
      end

      def privacy_node(node_id=nil)
        get_node(node_id||self.privacy_node_id)
      end

      def activities_node(node_id=nil)
        get_node(node_id||self.activities_node_id)
      end

      def activities_list_node(node_id=nil)
        get_node(node_id||self.activities_list_node_id)
      end
      ### NODES IDs FETCHING FINISHED ###

      def prepare_activity_string(activity)
        return activity.to_s.parameterize.underscore.to_s
      end

      def prepare_magnitude_value(magnitude)
        if magnitude
          magnitude = magnitude.try(:to_f).try(:round)
          if magnitude < 0
            return -1
          else
            return 1
          end
        else
          return 1
        end
      end

      def prepare_timestamp(timestamp)
        if timestamp && ["DateTime", "Time"].include?(timestamp.class.name)
          return timestamp.to_i
        else
          return DateTime.now.to_i
        end
      end

      ### ACTIONS TRACKING STARTED ###


      # User's distinct relations with count
      # options:
        # weights: true
        # complemented: true,
        # activity_types: ["dis liked", "dis-liked", "disliked", :disliked, :dis_liked] || "disliked" || :disliked

      def all_activities_types
        activities_types = $neo.list_relationship_types 
        activities_types = activities_types-["privacy", "activities", "activities_list"]
        return activities_types
      end

      def activities_counts(options={})
        self_node = options[:self_node] || self.entity_node
        activities_node = options[:activities_node] || self.activities_node(self_node[:activities_node_id])
        activities_counts = activities_node[:activities_counts]
        if activities_counts.nil?
          self_node_id = self_node.neo_id
          query = "start n=node(#{self_node_id.to_s}) match n-[r]->() return distinct(type(r)), count(r);"
          response = $neo.execute_query(query)
          activities_counts = response["data"]
          unless activities_counts.empty?
            activities_counts = Hash[ activities_counts.map{ |a| [a.first,a.last] } ]
            activities_node[:activities_counts] = YAML.dump(activities_counts)
          end
        else
          activities_counts = YAML.load(activities_counts)
        end
        activities_counts = activities_counts.sort.to_h

        if options[:weights]
          sum = activities_counts.inject(0) {|sum,y| sum+y[1]}.to_f
          activities_counts = Hash[ activities_counts.map{ |key,value| [key,((value.to_f/sum)*100)] } ]
        end
        if options[:activity_types]
          if options[:activity_types].is_a?(Array)
            activities = options[:activity_types].map{|at| prepare_activity_string(at)}.compact.uniq
            activities_counts = activities_counts.select{|k,v| activities.include?(k)}
          else
            activity = prepare_activity_string(options[:activity_types])
            activities_counts = activities_counts.select{|k,v| k==activity}
          end
        else
          all_activities = options[:all_activities_types] || all_activities_types
          acts_string = activities_counts.map{|k,v| prepare_activity_string(k)}
          rest_acts = all_activities-acts_string
          activities_counts = activities_counts.merge(Hash[rest_acts.map{|a|[a,0]}])
        end
        return activities_counts
      end


      ### ACTIONS TRACKING FINISHED ###


      ### SOCIAL MODELING STARTED ###

      # options:
        # weighted: true >> Then the input is in the form of [{entity: entity_node, weight: weight%}, ...]
        # weight: true >> Then the output is in the form of [{entity: entity_node, weight: weight%}, ...]
        # sort: true >> This means that the output should be sorted > The output will be in the form of [{entity: entity_object, weight: weight%}, ...]
        # if none from the above is passed as input: then the input is in the form of [entity_node, ...] >> and the result will be in the form of [entity_object, ...]

      def fetch_entities(entities_nodes_list, options={})
        if options[:only_ids]
          entities_nodes_list = entities_nodes_list.map{|n_id| Neography::Node.load(n_id, $neo) }.compact.uniq
        end

        if options[:weighted]
          all_recommendations = entities_nodes_list.map{|n| {entity: [n[:entity]["object_type"], n[:entity]["object_id"]], weight: n[:weight] } } 
          grouped_by_type = all_recommendations.group_by{|k,v| k[0]}
          results = []
          grouped_by_type.each do |grouped_entities|
            entities_ids = grouped_entities[1].map{|u| u[:entity][1]}.compact.uniq
            begin
              results << grouped_entities[0].safe_constantize.where(id: entities_ids).try(:to_a)
            rescue
            end
          end
          results = results.flatten.compact.uniq
          frs = []
          results.each do |result|
            rs = all_recommendations.select{|r| r[:entity] == [result.class.name, result.id]}.first
            if rs
              rs[:entity] = result 
              frs << rs
            end
          end
          if options[:sort]
            frs = frs.sort_by{ |h| h[:weight] }.reverse!
          end
          return frs
        else
          all_recommendations = entities_nodes_list.map{|n| [n["object_type"], n["object_id"]] }
          grouped_by_type = all_recommendations.group_by{|x| x[0]}
          results = []
          grouped_by_type.each do |grouped_entities|
            entities_ids = grouped_entities[1].map{|u| u[1]}.compact.uniq
            begin
              results << grouped_entities[0].safe_constantize.where(id: entities_ids).try(:to_a)
            rescue
            end
          end
          results = results.flatten.compact.uniq
          if options[:weight] || options[:sort]
            results = weight_entities(results, options)
            # fetch_entities(results, options.merge({weighted: true}))
          end          
          return results
        end
      end

      # options:
        # self_node: entity_node
        # self_relations_weights: [activity_type: weight, ...]
        # self_activities_list: [{timestamp: time, actionable_node: node, activity_type: activity_type}, ...]
        # sort: true
      def weight_entities(entities_list, options={})
        ratings = []

        self_node = options[:self_node] || self.entity_node
        self_activities_node = options[:activities_node] || self.activities_node(self_node[:activities_node_id])
        self_activities_weights = options[:relations_weights] || self.activities_counts(weights: true, activities_node: self_activities_node)
        self_activities_sets = options[:activities_sets] || YAML.load(self_activities_node[:activities_sets])
        self_activities_list_node = nil
        if options[:activities_list]
          self_activities_list = options[:activities_list]
        else
          self_activities_list_node = self.activities_list_node(self_node[:activites_list_node_id])
          self_activities_list = YAML.load(self_activities_list_node[:activities])
        end

        entities_list.each do |entity|
          ratings << {entity: entity, weight: self.calculate_relation(entity,self_node: self_node, activities_node: self_activities_node, activities_list_node: self_activities_list_node, self_weights: self_activities_weights, activities_sets: self_activities_sets, activities_list: self_activities_list)}
        end
        if options[:sort]
          ratings = ratings.sort_by{ |h| h[:weight] }.reverse!
        end
        return ratings
      end

      # To Track the entity
      def follow(followed)
        if followed.is_a?(Sociographer::Entity) && !self.follows?(followed)
          self.make_activity(actionable: followed, activity_type: "follow")
        end
      end

      # To unTrack the entity
      def unfollow(followed)
        if followed.is_a?(Sociographer::Entity)
          self_node = self.entity_node
          followed_node = followed.entity_node
          relation = $neo.get_node_relationships_to(self_node, followed_node, "out", "follow").first
          $neo.delete_relationship(relation) if relation.present?
        end
      end

      def get_follows(relation, options={})
        self_node = self.entity_node
        followers = []
        if relation == "followed"
          followers = self_node.outgoing("follow").map{|n| n}
        else
          followers = self_node.incoming("follow").map{|n| n}
        end
        unless options[:only_nodes]
          followers = fetch_entities(followers, options.merge({self_node: self_node}) )
        end
        return followers
      end

      # To get all trackers
      def followers(options={})
        get_follows("followers", options)
      end

      # To get all tracking entities
      def followed(options={})
        get_follows("followed", options)
      end

      # To get friends: The common entities between trackers and trackings
      def friends(options={})
        friends = self.followers(only_nodes: true) & self.followed(only_nodes: true)
        unless options[:only_nodes]
          friends = fetch_entities(friends, options)
        end
        return friends
      end

      # To check if tracking the entity or not
      def follows?(followed)
        if followed.is_a?(Sociographer::Entity)
          self_node_id = self.entity_node_id
          followed_node_id = followed.entity_node_id
          if self_node_id && followed_node_id
            query = "start n1=node(#{self_node_id.to_s}), n2=node(#{followed_node_id.to_s})  match n1-[r:follow]->n2 return r;"
            response = $neo.execute_query(query)
            unless response["data"].empty?
              true
            else
              false
            end
          end
        end
      end

      # To check if friend with the entity or net
      def friend?(followed)
        if followed.is_a?(Sociographer::Entity)
          if self.follows?(followed) && followed.follows?(self)
            true
          else
            false
          end
        end
      end

      # Suggest friends: friends of friends
      def friend_suggestions(limit=30)
        self_node = self.entity_node
        self_node_id = self_node.neo_id.to_i
        recommendations = self_node.incoming(:follow).order("breadth first").uniqueness("node global").depth(2).map{|n| n }.flatten.compact.uniq
        results = fetch_entities(recommendations, self_node: self_node, sort: true)
        limit = limit.try(:to_i)
        results = results.first(limit) if limit
        return results
      end

      ### SOCIAL MODELING FINISHED ###


      ### SOCIAL RECOMMENDATION STARTED ###

      # Get all paths of entities between you and the entity (like linkedin)
      def degrees_of_separation(entity, options={})
        if entity.is_a?(Sociographer::Entity)
          if options[:shortest]
            shortest_degrees_of_separation(entity)
          else
            self_node = self.entity_node
            entity_node = entity.entity_node
            paths = []
            found_entities = []
            self_node.all_simple_paths_to(entity_node).incoming(:follow).depth(5).nodes.each do |path|
              in_between = []
              path.each do |n|
                begin
                  ent = found_entities.select{|e| (e[:object_id] == n[:object_id]) && (e[:object_type] == n.object_type) }.first
                  if ent
                    ent = ent[:entity_record]
                  else
                    ent = n.object_type.safe_constantize.find_by(id: n[:object_id])
                    if ent
                      found_entities << {object_id: n[:object_id], object_type: n.object_type, entity_record: ent}
                    end
                  end
                rescue
                  ent = nil
                end
                in_between << ent
              end
              unless in_between.include?(nil)
                paths << {length: in_between.size-1, users: in_between}
              end
            end
            return paths
          end
        end
      end

      # Get the shortest path of entities between you and the entity (like linkedin)
      def shortest_degrees_of_separation(entity, options={})
        if entity.is_a?(Sociographer::Entity)
          self_node = self.entity_node
          entity_node = entity.entity_node
          if self_node && entity_node
            paths = []
            self_node.shortest_path_to(entity_node).incoming(:follow).depth(5).nodes.each do |path|
              in_between = path.map{|n| begin n.object_type.safe_constantize.find(n[:object_id]) rescue nil end }
              unless in_between.include?(nil)
                path = {length: path.size-1, users: in_between}
                paths << path
              end
            end
            return paths.first
          end
        end
      end

      ### SOCIAL RECOMMENDATION FINISHED ###

      def update_activities(options={})
        self_node = options[:self_node] || self.entity_node
        activities_node = options[:activities_node] || self.activities_node(self_node[:activities_node_id])
        activities_list_node = options[:activities_list_node] || self.activities_list_node(self_node[:activities_list_node_id])
        
        activities_list = options[:activities] || YAML.load(activities_list_node[:activities])
        if activities_list.empty?
          activities_list = []
          relations = self_node.rels.outgoing.map{|r| r}
          relations.each do |relation|
            act_type = relation.rel_type
            act_timestap = relation.timestamp
            act_node = relation.end_node.neo_id.to_i
            act_magnitude = relation.magnitude
            activity_representation = {timestamp: act_timestap, activity_node_id: act_node, activity_type: act_type, magnitude: act_magnitude}
            activities_list << activity_representation
          end
          activities_list = relations.sort_by{|r| r[:timestamp]}
          activities_list_node[:activities] = YAML.dump(activities_list)
        end
        activities_sum = activities_list.count
        activities_sets = []
        first_activity = activities_list.first
        last_activity = activities_list.last
        first_activity_timestamp = first_activity[:timestamp]
        last_activity_timestamp = last_activity[:timestamp]

        if activities_sum > 100 && ( (last_activity_timestamp-first_activity_timestamp) >= 1.month.to_i)
          activities_sublists = activities_list.each_slice(4).to_a
          activities_sublists.each_with_index do |list, index|
            list_sum = list.size.to_f
            first_activity = list.first
            last_activity = list.last
            first_activity_timestamp = first_activity[:timestamp]
            last_activity_timestamp = last_activity[:timestamp]
            list_grouped_activities = Hash[ list.group_by{|li| li[:activity_type]}.map{|act| [act[0], ((act[1].count/list_sum)*100)]} ]
            activities_sets << {list_index: index, starting_timestamp: first_activity_timestamp, ending_timestamp: last_activity_timestamp, list: list_grouped_activities }
          end
          activities_node[:past_activities_count] = (activities_sum > 0 ? activities_sum.to_i : 1)
          activities_node[:current_activities_count] = 0
        else
          activities_sets = []
          list_sum = activities_list.size.to_f
          list_grouped_activities = Hash[ activities_list.group_by{|li| li[:activity_type]}.map{|act| [act[0], ((act[1].count/list_sum)*100)]} ]
          activities_sets << {list_index: 0, starting_timestamp: first_activity_timestamp, ending_timestamp: last_activity_timestamp, list: list_grouped_activities }
        end
        activities_node[:activities_sets] = YAML.dump(activities_sets)
      end

      # Call it to make the relation between the entity and the actionable
      def make_activity(options={})
        if options[:actionable] && ( options[:actionable].is_a?(Sociographer::Actionable) || options[:actionable].is_a?(Sociographer::Entity) ) && options[:activity_type]

          ## Getting all user needed nodes and variables/lists
          self_node = self.entity_node
          self_activities_node = self.activities_node(self_node[:activities_node_id])
          self_activities_list_node = self.activities_list_node(self_node[:activities_list_node_id])

          self_activities_counts = YAML.load(self_activities_node[:activities_counts]) 
          self_activities_list = YAML.load(self_activities_list_node[:activities])
          ## Calculations activity representation variables
          actionable_node = options[:actionable].entity_node
          magnitude = prepare_magnitude_value(options[:magnitude])
          activity_type = prepare_activity_string(options[:activity_type])
          timestamp = prepare_timestamp(options[:timestamp])

          ## Updating activities counts
          if self_activities_counts[activity_type]
            self_activities_counts[activity_type] = self_activities_counts[activity_type]+1
          else
            self_activities_counts.merge!( Hash[[[activity_type, 1]]] )
          end
          self_activities_node[:activities_counts] = YAML.dump(self_activities_counts)

          ## Concatinating the activity representations to the list of activities done by the user
          activity_representation = {timestamp: timestamp, activity_node_id: actionable_node.neo_id.to_i, activity_type: activity_type, magnitude: magnitude}
          p activity_representation
          self_activities_list << activity_representation
          self_activities_list_node[:activities] = YAML.dump(self_activities_list)
          ## Creation of the relation in the graph db
          relation_relationship = Neography::Relationship.create(activity_type, self_node, actionable_node)
          $neo.set_relationship_properties(relation_relationship, {"magnitude" => magnitude, "timestamp" => timestamp})

          ## Updating the current activities count
          self_activities_node[:current_activities_count] += 1
          # if (self_activities_node[:past_activities_count] >= 70) && ( (self_activities_node[:current_activities_count]/self_activities_node[:past_activities_count].to_f) >= 0.05 )
            self.update_activities(self_node: self_node, activities_node: self_activities_node, activities_list_node: self_activities_list_node, activities: self_activities_list)
          # end
        end
      end

      def weight_checker(activity_timestamp, activity_type, activities_sets)
        if activities_sets.count < 4
          return activities_sets[0][:list][activity_type] || 0
        else 
          case
          when (activities_sets[0][:starting_timestamp] <= activity_timestamp) && (activity_timestamp < activities_sets[0][:ending_timestamp])
            weight = activities_sets[0][:list][activity_type] || 0
            return weight*0.25
          when (activities_sets[1][:starting_timestamp] <= activity_timestamp) && (activity_timestamp < activities_sets[1][:ending_timestamp])
            weight = activities_sets[1][:list][activity_type] || 0
            return weight*0.5
          when (activities_sets[2][:starting_timestamp] <= activity_timestamp) < (activity_timestamp < activities_sets[2][:ending_timestamp])
            weight = activities_sets[2][:list][activity_type] || 0
            return weight*0.75
          else
            return activities_sets[3][:list][activity_type] || 0
          end
        end
      end

      ### SOCIAL TRUST STARTED ###
      
      # To calculate a number representing the relation between you and the entity:
        # according to the weight of the relations, their magnitude, and their frequencies
        # according to each user
      def calculate_relation(entity, options={})
        if entity.is_a?(Sociographer::Entity) && (self!=entity)
          ## USER DATA
          self_node = options[:self_node] || self.entity_node
          self_node_id = self_node.neo_id.to_i
          
          self_activities_node = options[:activities_node] || self.activities_node(self_node[:activities_node_id])
          self_activities_weights = options[:self_weights] || self.activities_counts(weights: true, activities_node: self_activities_node)
          self_activities_sets = options[:activities_sets] || YAML.load(self_activities_node[:activities_sets])
          self_entities_weights = options[:entities_weights] || YAML.load(self_activities_node[:entities_weights])
          ##

          ## ENTITY DATA
          entity_node = entity.entity_node
          entity_node_id = entity_node.neo_id.to_i
          
          entity_activities_node = entity.activities_node(entity_node[:activities_node_id])
          entity_activities_weights = entity.activities_counts(weights: true, activities_node: entity_activities_node)
          entity_activities_sets = YAML.load(entity_activities_node[:activities_sets])
          entity_entities_weights = YAML.load(entity_activities_node[:entities_weights])
          ## 

          weight_in_between = nil

          follow_relation = self_entities_weights[entity_node_id]
          if follow_relation
            relation_weight = follow_relation[:weight]
            start_last_updated_count = follow_relation[:start_last_updated_count]
            entity_last_updated_count = follow_relation[:end_last_updated_count]
            if (relation_weight!=0) && ((start_last_updated_count/self_activities_node[:past_activities_count].to_f) <= 0.05) || ( (entity_last_updated_count/entity_activities_node[:past_activities_count].to_f) <= 0.05)
              weight_in_between = relation_weight
            end
          end

          unless weight_in_between

            self_activities_list_node = options[:activities_list_node] || self.activities_list_node(self_node[:activities_list_node_id])
            self_activities_list = options[:activities_list] || YAML.load(self_activities_list_node[:activities])
  
            entity_activities_list_node = entity.activities_list_node(entity[:activities_list_node_id])
            entity_activities_list = YAML.load(entity_activities_list_node[:activities])


            self_grouped_activities = self_activities_list.group_by{|a| a[:activity_node_id]}
            entity_grouped_activities = entity_activities_list.group_by{|a| a[:activity_node_id]}

            self_grouped_activities_nodes = self_grouped_activities.map{|a| a[0]}.compact.uniq
            entity_grouped_activities_nodes = entity_grouped_activities.map{|a| a[0]}.compact.uniq
            common_nodes = self_grouped_activities_nodes & entity_grouped_activities_nodes

            self_common_activities = self_grouped_activities.select{|k,v| common_nodes.include?(k)}
            entity_common_activities = entity_grouped_activities.select{|k,v| common_nodes.include?(k)}

            weight_in_between = 0
            self_common_activities.each do |actionable_node|
              self_node_acts = actionable_node[1]
              entity_node_acts = entity_common_activities[actionable_node[0]]
              
              self_actionable_node_acts_sum = 0
              self_node_acts.each do |act|
                activity_type = act[:activity_type]
                timestamp = act[:timestamp]
                magnitude = act[:magnitude]
                weight = (1-weight_checker(timestamp, activity_type, self_activities_sets))*magnitude
                self_actionable_node_acts_sum += weight
              end

              entity_actionable_node_acts_sum = 0
              entity_node_acts.each do |act|
                activity_type = act[:activity_type]
                timestamp = act[:timestamp]
                magnitude = act[:magnitude]
                weight = (1-weight_checker(timestamp, activity_type, entity_activities_sets))*magnitude
                entity_actionable_node_acts_sum += weight
              end

              weight_in_between += self_actionable_node_acts_sum*entity_actionable_node_acts_sum
            end
            self_entities_weights[entity_node_id] = {start_last_updated_count: self_activities_node[:past_activities_count], end_last_updated_count: entity_activities_node[:past_activities_count], weight: weight_in_between}
            entity_entities_weights[self_node_id] = {start_last_updated_count: entity_activities_node[:past_activities_count], end_last_updated_count: self_activities_node[:past_activities_count], weight: weight_in_between}
            
            self_activities_node[:entities_weights] = YAML.dump(self_entities_weights)
            entity_activities_node[:entities_weights] = YAML.dump(entity_entities_weights)
          end
          if weight_in_between > self_entity[:top_follower_weight]
            self_entity[:top_follower_weight] = weight_in_between
          end
          return weight_in_between

        end
      end

      def profile_feed(page=1, per_page=30)
        if per_page < 1
          per_page = 20
        end
        skipped_no = (page-1)*per_page
        if skipped_no < 0
          skipped_no = 0
        end

        self_node_id = self.get_node_id
        qur = "start n=node("+self_node_id.to_s+") match n-[r]->(z) return type(r), z, r.created_at ORDER BY r.created_at DESC SKIP #{skipped_no.to_s} LIMIT #{per_page}"
        response = $neo.execute_query(qur)
        feeds = []
        self_id = self.id
        self_type = self.class.name
        response["data"].each do |result|
          activity_type = result[0]
          attachment_data = result[1]["data"]
          activity_timestamp = Time.at(result[2]).to_datetime
          if activity_type && attachment_data && activity_timestamp
            feeds << FeedItem.new(activity_type, attachment_data["object_id"], attachment_data["object_type"], self_id, self_type, activity_timestamp, self)
          end
        end
        f_by_attachment = feeds.group_by{|x| x.attachment_type}
        f_by_attachment.each do |fbt|
          attachments_ids = fbt[1].map{|u| u.attachment_id}.compact.uniq
          attachments = fbt[0].safe_constantize.where(id: attachments_ids).to_a
          fbt[1].each do |feed_item|
            fi_attachment = attachments.select{ |a| a.id == feed_item.attachment_id}.first
            if fi_attachment
              feed_item.update_attachment(fi_attachment)
            else
              feed_item = nil
            end
          end
        end
        feeds = f_by_attachment.map{|u| u[1]}.flatten.compact
        feeds.sort_by{ |f| f.timestamp }
        feeds
      end

      def feed(page=1, per_page=30)
        if per_page < 1
          per_page = 20
        end
        skipped_no = (page-1)*per_page
        if skipped_no < 0
          skipped_no = 0
        end

        self_node_id = self.get_node_id
        qur = "start x=node("+self_node_id.to_s+") match x-[r:friends]->(y)-[r2]->(z) return type(r2), y, z, r2.created_at ORDER BY r2.created_at DESC SKIP #{skipped_no.to_s} LIMIT #{per_page}"
        response = $neo.execute_query(qur)        
        feeds = []
        response["data"].each do |result|
          activity_type = result[0]
          actor_data = result[1]["data"]
          # actor = actor_data["object_type"].safe_constantize.find_by(id: actor_data["object_id"])
          attachment_data = result[2]["data"]
          # attachment = attachment_data["object_type"].safe_constantize.find_by(id: attachment_data["object_id"])
          activity_timestamp = Time.at(result[3]).to_datetime
          if activity_type && actor_data && attachment_data && activity_timestamp
            feeds << FeedItem.new(activity_type, attachment_data["object_id"], attachment_data["object_type"], actor_data["object_id"], actor_data["object_type"], activity_timestamp)
          end
        end
        f_by_attachment = feeds.group_by{|x| x.attachment_type}
        f_by_attachment.each do |fbt|
          attachments_ids = fbt[1].map{|u| u.attachment_id}.compact.uniq
          attachments = fbt[0].safe_constantize.where(id: attachments_ids).to_a
          fbt[1].each do |feed_item|
            fi_attachment = attachments.select{ |a| a.id == feed_item.attachment_id}.first
            if fi_attachment
              feed_item.update_attachment(fi_attachment)
            else
              feed_item = nil
            end
          end
        end
        f_by_actor = f_by_attachment.map{|u| u[1]}.flatten.compact
        f_by_actor = f_by_actor.group_by{|x| x.actor_type}
        f_by_actor.each do |fbt|
          actors_ids = fbt[1].map{|u| u.actor_id}.compact.uniq
          actors = fbt[0].safe_constantize.where(id: actors_ids).to_a
          fbt[1].each do |feed_item|
            fi_actor = actors.select{ |a| a.id == feed_item.actor_id}.first
            if fi_actor
              feed_item.update_actor(fi_actor)
            else
              feed_item = nil
            end
          end
        end
        feeds = f_by_actor.map{|u| u[1]}.flatten.compact
        feeds.sort_by{ |f| f.timestamp }
        feeds
      end

      def get_entity_lists(entity, options={})
        if entity.is_a?(Sociographer::Entity)
          self_node = options[:self_node] || self.entity_node
          privacy_node = options[:privacy_node] || self.privacy_node(self_node[:privacy_node_id])

          entity_node_id = entity.entity_node_id
          
          all_lists = get_all_lists_nodes(self_node: self_node, privacy_node: privacy_node)
          selected_lists = all_lists.select{ |list| list[1].include?(entity_node_id) }.map{|list| list[0]}
          return selected_lists
        end
      end

      def ban(entity, options={})
        if entity.is_a?(Sociographer::Entity)
          self_node = options[:self_node] || self.entity_node
          privacy_node = options[:privacy_node] || self.privacy_node(self_node[:privacy_node_id])

          entity_node_id = entity.entity_node_id
          
          banned_list = YAML.load(privacy_node[:banned_list])
          banned_list << entity_node_id
          banned_list = banned_list.compact.uniq

          favorite_list = YAML.load(privacy_node[:favorite_list])
          favorite_list.delete(entity_node_id)

          privacy_node[:banned_list] = YAML.dump(banned_list)
          privacy_node[:favorite_list] = YAML.dump(favorite_list)
        end
      end

      def unban(entity, options={})
        if entity.is_a?(Sociographer::Entity)
          self_node = options[:self_node] || self.entity_node
          privacy_node = options[:privacy_node] || self.privacy_node(self_node[:privacy_node_id])

          entity_node_id = entity.entity_node_id
          
          banned_list = YAML.load(privacy_node[:banned_list])
          banned_list.delete(entity_node_id)
          privacy_node[:banned_list] = YAML.dump(banned_list)
        end
      end

      def get_banned(options={})
        self_node = options[:self_node] || self.entity_node
        privacy_node = options[:privacy_node] || self.privacy_node(self_node[:privacy_node_id])
        
        banned_list = YAML.load(privacy_node[:banned_list])
        if options[:only_nodes]
          return banned_list
        else
          return fetch_entities(banned_list, only_ids: true)
        end
      end

      def add_favorite(entity, options={})
        if entity.is_a?(Sociographer::Entity)
          self_node = options[:self_node] || self.entity_node
          privacy_node = options[:privacy_node] || self.privacy_node(self_node[:privacy_node_id])

          entity_node_id = entity.entity_node_id
          
          favorite_list = YAML.load(privacy_node[:favorite_list])
          favorite_list << entity_node_id
          favorite_list = favorite_list.compact.uniq

          banned_list = YAML.load(privacy_node[:banned_list])
          banned_list.delete(entity_node_id)

          privacy_node[:banned_list] = YAML.dump(banned_list)
          privacy_node[:favorite_list] = YAML.dump(favorite_list)
        end
      end

      def remove_favorite(entity, options={})
        if entity.is_a?(Sociographer::Entity)
          self_node = options[:self_node] || self.entity_node
          privacy_node = options[:privacy_node] || self.privacy_node(self_node[:privacy_node_id])

          entity_node_id = entity.entity_node_id
          
          favorite_list = YAML.load(privacy_node[:favorite_list])
          favorite_list.delete(entity_node_id)
          privacy_node[:favorite_list] = YAML.dump(favorite_list)
        end
      end

      def get_favorite(options={})
        self_node = options[:self_node] || self.entity_node
        privacy_node = options[:privacy_node] || self.privacy_node(self_node[:privacy_node_id])
        
        favorite_list = YAML.load(privacy_node[:favorite_list])
        if options[:only_nodes]
          return favorite_list
        else
          return fetch_entities(favorite_list, only_ids: true)
        end
      end

      def create_list(list_name,options={})
        if list_name.is_a?(String) && !list_name.strip.empty?
          self_node = options[:self_node] || self.entity_node
          privacy_node = options[:privacy_node] || self.privacy_node(self_node[:privacy_node_id])
          list_name = prepare_activity_string(list_name)
          unless privacy_node[list_name] || ["banned_list", "favorite_list"].include?(list_name)
            privacy_node[list_name] = YAML.dump []
          else
            false
          end
        end
      end

      def add_to_list(entity, list_name, options={})
        if entity.is_a?(Sociographer::Entity) && list_name.is_a?(String) && !list_name.strip.empty?
          self_node = options[:self_node] || self.entity_node
          privacy_node = options[:privacy_node] || self.privacy_node(self_node[:privacy_node_id])

          entity_node_id = entity.entity_node_id
          
          list_name = prepare_activity_string(list_name)

          unless ["banned_list", "favorite_list"].include?(list_name) || privacy_node[list_name] 
            privacy_node[list_name] = YAML.dump []
          end
          if privacy_node[list_name] 
            list = YAML.load(privacy_node[list_name]) 
            list << entity_node_id
            list = list.compact.uniq
            privacy_node[list_name] = YAML.dump(list)
          end
        end
      end

      def remove_from_list(entity, list_name, options={})
        if entity.is_a?(Sociographer::Entity) && list_name.is_a?(String) && !list_name.strip.empty?
          self_node = options[:self_node] || self.entity_node
          privacy_node = options[:privacy_node] || self.privacy_node(self_node[:privacy_node_id])

          entity_node_id = entity.entity_node_id
          
          list_name = prepare_activity_string(list_name)

          if !["banned_list", "favorite_list"].include?(list_name) && privacy_node[list_name]
            list = YAML.load(privacy_node[list_name]) 
            list.delete(entity_node_id)
            privacy_node[list_name] = YAML.dump(list)
          end

        end
      end

      def get_list(list_name, options={})
        if list_name.is_a?(String) && !list_name.strip.empty?
          self_node = options[:self_node] || self.entity_node
          privacy_node = options[:privacy_node] || self.privacy_node(self_node[:privacy_node_id])
          
          list_name = prepare_activity_string(list_name)

          if privacy_node[list_name]
            list = YAML.load(privacy_node[list_name])
            if options[:only_nodes]
              return list
            else
              return fetch_entities(list, only_ids: true)
            end
          end
        end
      end

      def delete_list(list_name, options={})
        if list_name.is_a?(String) && !list_name.strip.empty?
          self_node = options[:self_node] || self.entity_node
          privacy_node = options[:privacy_node] || self.privacy_node(self_node[:privacy_node_id])
          
          list_name = prepare_activity_string(list_name)
          if privacy_node[list_name]
            $neo.remove_node_properties(privacy_node, list_name)
          end 
        end
      end

      def get_all_lists_names(options={})
        self_node = options[:self_node] || self.entity_node
        privacy_node = options[:privacy_node] || self.privacy_node(self_node[:privacy_node_id])

        lists = $neo.get_node_properties(privacy_node)
        lists.except!(:banned_list, :favorite_list, "object_type", "refrence_id", "refrence_type")
        return lists.map{|u|u[0]}
      end

      def get_all_lists_nodes(options={})
        all_lists_names = get_all_lists_names(self_node: self_node, privacy_node: privacy_node)
        all_lists = all_lists_names.map{|ln| HASH[ [ln, get_list(ln, only_nodes: true, self_node: self_node, privacy_node: privacy_node)] ]}
        all_lists << [{"banned_list" => self.get_banned(only_nodes: true, self_node: self_node, privacy_node: privacy_node)}, {"favorite_list" => self.get_favorite(only_nodes: true, self_node: self_node, privacy_node: privacy_node)}]
        all_lists.flatten!
      end

      def classify(entity, options={})
        if entity.is_a?(Sociographer::Entity)

          all_activities = all_activities_types

          self_node = options[:self_node] || self.entity_node
          privacy_node = options[:privacy_node] || self.privacy_node(self_node[:privacy_node_id])

          entity_node_id = entity.entity_node_id

          entity_acts_counts = Hash[ entity.activities_counts(all_activities_types: all_activities).sort ]
          entity_acts_counts_list = entity_acts_counts.map{|eac| eac[1]}

          all_lists = get_all_lists_nodes(self_node: self_node, privacy_node: privacy_node)
          all_nodes_ids = all_lists.map{|list| list[1]}.flatten.uniq
          all_entities = fetch_entities(all_nodes_ids, only_ids: true)

          all_entities_acts_counts = all_entities.map{|e| {entity: e, acts_counts: Hash[ e.activities_counts(all_activities_types: all_activities).sort ] } }

          knn_data = all_entities_acts_counts.map{|e| {id: "#{e[:entity].class.name}#{e[:entity].id}", point: e[:acts_counts].map{|ac| ac[1]} } }
          index = KnnBall.build(data)
          result = index.nearest(entity_acts_counts_list)
          result_node_string = result[:id].split("#")
          result_entity = result_node_string[0].safe_constantize.find_by(id: result_node_string[1])
          result_lists = get_entity_lists(result_entity)
          return {similar_entity: result_entity, predicted_lists: result_lists}
        end
      end

      # To Ensure updating the cached relation index in all tracking entities' nodes
      def ensure_deletion_fixes
        self_node = self.entity_node
        privacy_node = self.privacy_node(self_node[:privacy_node_id])
        activities_node = self.activities_node(self_node[:activities_node_id])
        activities_list_node = self.activities_list_node(self_node[:activities_list_node_id])

        $neo.remove_node_from_index("entities_nodes_index", self_node)
        self_node.del
        privacy_node.del
        activities_node.del
        activities_list_node.del
      end

    end
  end
end
