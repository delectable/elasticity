module Elasticity

  class JobFlowStatusStep

    attr_accessor :name
    attr_accessor :state
    attr_accessor :created_at
    attr_accessor :started_at
    attr_accessor :ended_at

    # Create a job flow from an AWS <member> (Nokogiri::XML::Element):
    #   /DescribeJobFlowsResponse/DescribeJobFlowsResult/JobFlows/member/Steps/member
    def self.from_member_element(xml_element)
      job_flow_step = JobFlowStatusStep.new
      job_flow_step.name = xml_element.xpath('./StepConfig/Name').text.strip
      job_flow_step.state = xml_element.xpath('./ExecutionStatusDetail/State').text.strip
      created_at = xml_element.xpath('./ExecutionStatusDetail/CreationDateTime').text.strip
      job_flow_step.created_at = (created_at == '') ? (nil) : (Time.parse(created_at))
      started_at = xml_element.xpath('./ExecutionStatusDetail/StartDateTime').text.strip
      job_flow_step.started_at = (started_at == '') ? (nil) : (Time.parse(started_at))
      ended_at = xml_element.xpath('./ExecutionStatusDetail/EndDateTime').text.strip
      job_flow_step.ended_at = (ended_at == '') ? (nil) : (Time.parse(ended_at))
      job_flow_step
    end

    # Create JobFlowSteps from a collection of AWS <member> nodes (Nokogiri::XML::NodeSet):
    #   /DescribeJobFlowsResponse/DescribeJobFlowsResult/JobFlows/member/Steps/member
    def self.from_members_nodeset(members_nodeset)
      jobflow_steps = []
      members_nodeset.each do |member|
        jobflow_steps << from_member_element(member)
      end
      jobflow_steps
    end

    def self.from_step_hash(step_hash)
      job_flow_step = JobFlowStatusStep.new
      job_flow_step.name = step_hash['StepConfig']['Name'].to_s.strip
      job_flow_step.state = step_hash['ExecutionStatusDetail']['State'].to_s.strip
      created_at = step_hash['ExecutionStatusDetail']['CreationDateTime'].to_i
      job_flow_step.created_at = (created_at == 0) ? (nil) : (Time.at(created_at))
      started_at = step_hash['ExecutionStatusDetail']['StartDateTime'].to_i
      job_flow_step.started_at = (started_at == 0) ? (nil) : (Time.at(started_at))
      ended_at = step_hash['ExecutionStatusDetail']['EndDateTime'].to_i
      job_flow_step.ended_at = (ended_at == 0) ? (nil) : (Time.at(ended_at))
      job_flow_step
    end

    def self.from_step_hashes(step_hashes)
      step_statuses = []
      step_hashes.each do |step_hash|
        step_statuses << from_step_hash( step_hash )
      end
      step_statuses
    end
  end
end
