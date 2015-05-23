# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'sociographer/version'

Gem::Specification.new do |spec|
  spec.name          = "sociographer"
  spec.version       = Sociographer::VERSION
  spec.authors       = ["Hazem ElKilisly", "Islam Wazery"]
  spec.email         = ["wazery@ubuntu.com"]

  spec.summary       = %q{Ruby social engine based on Neo4j.}
  spec.description   = %q{Ruby social engine based on Neo4j.}
  spec.homepage      = "https://github.com/wazery/sociographer"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  if spec.respond_to?(:metadata)
    spec.metadata['allowed_push_host'] = "TODO: Set to 'http://mygemserver.com' to prevent pushes to rubygems.org, or delete to allow pushes to any server."
  end

  spec.add_development_dependency "bundler", "~> 1.9"
  spec.add_development_dependency "rake", "~> 10.0"

	s.add_dependency("neography")
	s.add_dependency("knnball")
end
