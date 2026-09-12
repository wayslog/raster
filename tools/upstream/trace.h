// Fixed version cross-implementation trajectory;and tests/support/trace.rs Use the same text protocol.
#pragma once
#include <charconv>
#include <cstdint>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>
namespace comparison {
using Bytes = std::vector<uint8_t>;
inline uint64_t number(const std::string& text) {
  uint64_t n = 0;
  auto result = std::from_chars(text.data(), text.data() + text.size(), n);
  if (text.empty() || result.ec != std::errc{} || result.ptr != text.data()+text.size())
    throw std::runtime_error("Invalid unsigned integer");
  return n;
}
inline bool flag(const std::string& text) {
  if (text != "0" && text != "1") throw std::runtime_error("The flag must be 0 or 1");
  return text == "1";
}
inline Bytes unhex(const std::string& text) {
  if (text == "-") return {};
  if (text.empty() || text.size()%2 || text.size()>16*1024*1024)
    throw std::runtime_error("Invalid or exceeded hexadecimal length");
  Bytes result;
  auto digit=[](char c)->uint8_t {
    if(c>='0' && c<='9') return c-'0';
    if(c>='a' && c<='f') return c-'a'+10;
    if(c>='A' && c<='F') return c-'A'+10;
    throw std::runtime_error("Invalid hexadecimal character");
  };
  for (size_t i=0;i<text.size();i+=2) result.push_back(digit(text[i])*16+digit(text[i+1]));
  return result;
}
inline std::string hex(const Bytes& bytes) {
  if(bytes.empty()) return "-";
  static constexpr char digits[]="0123456789abcdef";
  std::string text; text.reserve(bytes.size()*2);
  for(uint8_t b:bytes){ text+=digits[b>>4]; text+=digits[b&15]; }
  return text;
}
struct Operand {
  bool numeric=true;
  uint64_t number=0;
  Bytes bytes;
};
struct Step {
  uint64_t session, serial;
  Bytes key;
  std::string operation;
  bool option=false;
  Operand operand;
};
struct Trace {
  uint64_t seed;
  std::vector<Step> steps;
};
inline Trace decode(const std::string& path) {
  std::ifstream file(path);
  if(!file) throw std::runtime_error("Unable to open track");
  std::string line;
  if(!std::getline(file,line)) throw std::runtime_error("Missing track header");
  std::istringstream header(line);
  std::string name,version,seed,extra;
  if(!(header>>name>>version>>seed) || header>>extra || name!="raster-trace" || version!="1")
    throw std::runtime_error("Invalid track header or version");
  Trace trace{number(seed),{}};
  size_t line_number=1;
  while(std::getline(file,line)) {
    ++line_number;
    try {
      std::istringstream stream(line);
      std::string session,serial,key,operation,option,kind,value;
      if(!(stream>>session>>serial>>key>>operation)) throw std::runtime_error("Operation field missing");
      Step step{number(session),number(serial),unhex(key),operation};
      if(step.key.size()>65536) throw std::runtime_error("Control key exceeds 64 KiB");
      if(operation=="read" || operation=="delete" || operation=="rmw") {
        if(!(stream>>option)) throw std::runtime_error("Missing action options");
        step.option=flag(option);
      }
      if(operation=="upsert" || operation=="rmw") {
        if(!(stream>>kind>>value)) throw std::runtime_error("Missing operation value");
        if(kind=="u") step.operand.number=number(value);
        else if(kind=="b") {step.operand.numeric=false;step.operand.bytes=unhex(value);}
        else throw std::runtime_error("Invalid value type");
      } else if(operation!="read" && operation!="delete") throw std::runtime_error("Unknown operation");
      if(stream>>extra) throw std::runtime_error("redundant operation fields");
      if(trace.steps.size()>=2000000) throw std::runtime_error("Trajectories over two million steps");
      trace.steps.push_back(std::move(step));
    } catch(const std::exception& e) {
      throw std::runtime_error("Track No. "+std::to_string(line_number)+" OK:"+e.what());
    }
  }
  if(!file.eof()) throw std::runtime_error("Track reading failed");
  return trace;
}
}
