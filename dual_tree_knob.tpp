#define TYPE int
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
        std::ifstream infile(CONFIG_FILE_PATH);
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
std::string DUAL_TREE_KNOBS<_key, _value>::CONFIG_FILE_PATH = "";


template<typename _key, typename _value>
float DUAL_TREE_KNOBS<_key, _value>::SORTED_TREE_SPLIT_FRAC(){ 
    return std::stof(DUAL_TREE_KNOBS<_key, _value>::config_get_or_default("SORTED_TREE_SPLIT_FRAC", "1.0"));
}

template<typename _key, typename _value>
float DUAL_TREE_KNOBS<_key, _value>::UNSORTED_TREE_SPLIT_FRAC(){
    return std::stof(DUAL_TREE_KNOBS<_key, _value>::config_get_or_default("UNSORTED_TREE_SPLIT_FRAC", "0.5"));
}

template<typename _key, typename _value>
bool DUAL_TREE_KNOBS<_key, _value>::ENABLE_LAZY_MOVE(){ 
    return str2bool(DUAL_TREE_KNOBS<_key, _value>::config_get_or_default("ENABLE_LAZY_MOVE", "true"));
}

template<typename _key, typename _value>
bool DUAL_TREE_KNOBS<_key, _value>::ENABLE_OUTLIER_DETECTOR(){ 
    return str2bool(DUAL_TREE_KNOBS<_key, _value>::config_get_or_default("ENABLE_OUTLIER_DETECTOR", "true"));
}

template<typename _key, typename _value>
uint DUAL_TREE_KNOBS<_key, _value>::HEAP_SIZE(){ 
    return std::stof(DUAL_TREE_KNOBS<_key, _value>::config_get_or_default("HEAP_SIZE", "15"));
}

template<typename _key, typename _value>
float DUAL_TREE_KNOBS<_key, _value>::STD_TOLORANCE_FACTOR(){
    return std::stof(DUAL_TREE_KNOBS<_key, _value>::config_get_or_default("STD_TOLORANCE_FACTOR", "1.5"));
}

template<typename _key, typename _value>
uint DUAL_TREE_KNOBS<_key, _value>::NUM_STDEV(){ 
    return std::stof(DUAL_TREE_KNOBS<_key, _value>::config_get_or_default("NUM_STDEV", "3"));
}


template<typename _key, typename _value>
uint DUAL_TREE_KNOBS<_key, _value>::INIT_TOLERANCE_FACTOR(){ 
    return std::stof(DUAL_TREE_KNOBS<_key, _value>::config_get_or_default("INIT_TOLERANCE_FACTOR", "100"));
}

template<typename _key, typename _value>
float DUAL_TREE_KNOBS<_key, _value>::MIN_TOLERANCE_FACTOR(){ 
    return std::stof(DUAL_TREE_KNOBS<_key, _value>::config_get_or_default("MIN_TOLERANCE_FACTOR", "20"));
}

template<typename _key, typename _value>
float DUAL_TREE_KNOBS<_key, _value>::EXPECTED_AVG_DISTANCE(){
    return std::stof(DUAL_TREE_KNOBS<_key, _value>::config_get_or_default("EXPECTED_AVG_DISTANCE", "2.5"));
}

template<typename _key, typename _value>
TYPE DUAL_TREE_KNOBS<_key, _value>::LAST_K_STDEV() { 
    return std::stof(DUAL_TREE_KNOBS<_key, _value>::config_get_or_default("LAST_K_STDEV", "0"));
}

template<typename _key, typename _value>
TYPE DUAL_TREE_KNOBS<_key, _value>::OUTLIER_DETECTOR_TYPE(){ 
    return std::stoi(DUAL_TREE_KNOBS<_key, _value>::config_get_or_default("OUTLIER_DETECTOR_TYPE", "1"));
}
