#define NS_PRIVATE_IMPLEMENTATION
#define MTL_PRIVATE_IMPLEMENTATION
#include "GpuContext.hpp"
// TODO: remove the implementation macros when using this in other .mm files
GpuContext& GpuContext::instance() {
    static GpuContext ctx;
    return ctx;
}

GpuContext::GpuContext() {
    dev_ = MTL::CreateSystemDefaultDevice();
    if (!dev_) {
        auto* arr = MTL::CopyAllDevices();
        if (arr && arr->count() > 0) {
            dev_ = static_cast<MTL::Device*>(arr->object(0));
            dev_->retain();
        }
        if (arr) arr->release();
    }
    if (dev_) q_ = dev_->newCommandQueue();
}

GpuContext::~GpuContext() {
    if (q_) q_->release();
    if (dev_) dev_->release();
}