Dir.glob("#{__dir__}/**/test_*.rb").each do |path|
  load path
end
