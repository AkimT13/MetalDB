#pragma once
#include <../metal-cpp/Metal/Metal.hpp>
#include <../metal-cpp/Foundation/Foundation.hpp>

class GpuContext {
public:
    static GpuContext& instance();

    MTL::Device* device() const { return dev_; }
    MTL::CommandQueue* queue() const { return q_; }

    // Future: cache libraries and pipelines by name keys
    // Example: MTL::ComputePipelineState* pipeline(const char* key);

private:
    GpuContext();
    ~GpuContext();
    GpuContext(const GpuContext&) = delete;
    GpuContext& operator=(const GpuContext&) = delete;

    MTL::Device* dev_ = nullptr;
    MTL::CommandQueue* q_ = nullptr;
};