# frozen_string_literal: true

require_relative "lib/chronos/version"

Gem::Specification.new do |spec|
  spec.name = "chronos"
  spec.version = Chronos::VERSION
  spec.authors = ["Chi Man Lei"]
  spec.email = ["chimanlei@gmail.com"]

  spec.summary = "Chronos manages data archive process."
  spec.description = "Chronos provides a simple but effective way to manage data archive process."
  spec.homepage = ""
  spec.license = "MIT"
  spec.required_ruby_version = ">= 2.6.0"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(__dir__) do
    `git ls-files -z`.split("\x0").reject do |f|
      (f == __FILE__) || f.match(%r{\A(?:(?:bin|test|spec|features)/|\.(?:git|circleci)|appveyor)})
    end
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_runtime_dependency "ulid", ">= 1.3.0"
  spec.add_runtime_dependency "citrine", ">= 0.1.0"
end
