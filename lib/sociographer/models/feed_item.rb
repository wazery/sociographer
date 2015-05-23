module Sociographer
  class FeedItem
    attr_reader :activity_type, :attachment, :attachment_id, :attachment_type, :actor, :actor_id, :actor_id, :actor_type, :timestamp

    def initialize(activity_type, attachment_id, attachment_type, actor_id, actor_type, timestamp, actor=nil)
      @activity_type = activity_type
      @attachment = nil
      @attachment_id = attachment_id
      @attachment_type = attachment_type
      @actor = actor
      @actor_id = actor_id
      @actor_type = actor_type
      @timestamp = timestamp
    end

    def update_attachment(attachment)
      @attachment = attachment
    end

    def update_actor(actor)
      @actor = actor
    end
  end
end
