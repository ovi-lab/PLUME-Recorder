using System.Collections.Generic;
using PLUME.Core.Recorder;
using PLUME.Core.Recorder.Module.Frame;
using PLUME.Sample.Unity;
using UnityEngine.Scripting;
using MeshRendererSafeRef = PLUME.Core.Object.SafeRef.IComponentSafeRef<UnityEngine.MeshRenderer>;
using static PLUME.Core.Utils.SampleUtils;

namespace PLUME.Base.Module.Unity.Renderer.MeshRenderer
{
    [Preserve]
    public class MeshRendererRecorderModule : RendererRecorderModule<UnityEngine.MeshRenderer, MeshRendererFrameData>
    {
        private readonly Dictionary<MeshRendererSafeRef, MeshRendererCreate> _createSamples = new();
        private readonly Dictionary<MeshRendererSafeRef, MeshRendererDestroy> _destroySamples = new();

        protected override void OnObjectMarkedCreated(MeshRendererSafeRef objSafeRef, RecorderContext ctx)
        {
            base.OnObjectMarkedCreated(objSafeRef, ctx);
            _createSamples[objSafeRef] = new MeshRendererCreate { Component = GetComponentIdentifierPayload(objSafeRef) };
        }

        protected override void OnObjectMarkedDestroyed(MeshRendererSafeRef objSafeRef, RecorderContext ctx)
        {
            base.OnObjectMarkedDestroyed(objSafeRef, ctx);
            _destroySamples[objSafeRef] = new MeshRendererDestroy { Component = GetComponentIdentifierPayload(objSafeRef) };
        }

        protected override MeshRendererFrameData CollectFrameData(FrameInfo frameInfo, RecorderContext ctx)
        {
            var frameData = MeshRendererFrameData.Pool.Get();
            frameData.AddCreateSamples(_createSamples.Values);
            frameData.AddDestroySamples(_destroySamples.Values);
            frameData.AddUpdateSamples(GetRendererUpdateSamples());
            return frameData;
        }

        protected override void AfterCollectFrameData(FrameInfo frameInfo, RecorderContext ctx)
        {
            base.AfterCollectFrameData(frameInfo, ctx);
            _createSamples.Clear();
            _destroySamples.Clear();
        }
    }
}