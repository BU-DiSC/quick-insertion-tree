// Some helper functions
std::string str2lower(std::string str) {
    std::transform(str.begin(), str.end(), str.begin(), [](unsigned char c){ return std::tolower(c); });
    return str;
}

bool string_cmp_ignore_case(std::string str1, std::string str2) {
    return str2lower(str1) == str2lower(str2);
}

bool str2bool(std::string str) {
    return string_cmp_ignore_case(str, "true");
}

/* static */
template<typename _key, typename _value>
std::unordered_map<std::string, std::string>& DUAL_TREE_KNOBS<_key, _value>::get_config() {
    static auto config = new std::unordered_map<std::string, std::string>();
    if(config->empty()) {
        std::ifstream infile("./config");
        std::string line;
        while(std::getline(infile, line)) {
            line.erase(std::remove_if(line.begin(), line.end(), isspace), line.end());
            if(line.size() != 0) {
                if(line[0] != '#') {
                    // This line is not comment
                    std::string knob_name = line.substr(0, line.find("="));
                    std::string knob_val = line.substr(line.find("=") + 1, line.size());
                    config->insert({knob_name, knob_val});
                }
            }
        }
    }
    return *config;
}

/* static */
template<typename _key, typename _value>
std::string DUAL_TREE_KNOBS<_key, _value>::config_get_or_default(std::string knob_name, std::string default_val) {
    auto tmp = get_config();
    if(tmp.find(knob_name) == tmp.end()) {
        return default_val;
    }
    return tmp.at(knob_name);
}

template<typename _key, typename _value>
const float DUAL_TREE_KNOBS<_key, _value>::SORTED_TREE_SPLIT_FRAC = 
    std::stof(DUAL_TREE_KNOBS<_key, _value>::config_get_or_default("SORTED_TREE_SPLIT_FRAC", "1.0"));

template<typename _key, typename _value>
const float DUAL_TREE_KNOBS<_key, _value>::UNSORTED_TREE_SPLIT_FRAC =
    std::stof(DUAL_TREE_KNOBS<_key, _value>::config_get_or_default("UNSORTED_TREE_SPLIT_FRAC", "0.5"));

template<typename _key, typename _value>
const bool DUAL_TREE_KNOBS<_key, _value>::ENABLE_LAZY_MOVE = 
    str2bool(DUAL_TREE_KNOBS<_key, _value>::config_get_or_default("ENABLE_LAZY_MOVE", "true"));


